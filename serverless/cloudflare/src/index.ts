import {
  Compression,
  EtagMismatch,
  PMTiles,
  RangeResponse,
  ResolvedValueCache,
  Source,
  TileType,
} from "../../../js/index";
import { pmtiles_path, tile_path } from "../../shared/index";

interface Env {
  // biome-ignore lint: config name
  ALLOWED_ORIGINS?: string;
  // biome-ignore lint: config name
  BUCKET: R2Bucket;
  // d1 binding
  // biome-ignore lint: config name
  OCAP2_DATA: D1Database;
  // biome-ignore lint: config name
  CACHE_CONTROL?: string;
  // biome-ignore lint: config name
  PMTILES_PATH?: string;
  // biome-ignore lint: config name
  PUBLIC_HOSTNAME?: string;
}

class KeyNotFoundError extends Error { }

async function nativeDecompress(
  buf: ArrayBuffer,
  compression: Compression
): Promise<ArrayBuffer> {
  if (compression === Compression.None || compression === Compression.Unknown) {
    return buf;
  }
  if (compression === Compression.Gzip) {
    const stream = new Response(buf).body;
    const result = stream?.pipeThrough(new DecompressionStream("gzip"));
    return new Response(result).arrayBuffer();
  }
  throw Error("Compression method not supported");
}

const CACHE = new ResolvedValueCache(25, undefined, nativeDecompress);

class R2Source implements Source {
  env: Env;
  archiveName: string;

  constructor(env: Env, archiveName: string) {
    this.env = env;
    this.archiveName = archiveName;
  }

  getKey() {
    return this.archiveName;
  }

  async getBytes(
    offset: number,
    length: number,
    signal?: AbortSignal,
    etag?: string
  ): Promise<RangeResponse> {
    const resp = await this.env.BUCKET.get(
      pmtiles_path(this.archiveName, this.env.PMTILES_PATH),
      {
        range: { offset: offset, length: length },
        onlyIf: { etagMatches: etag },
      }
    );
    if (!resp) {
      throw new KeyNotFoundError("Archive not found");
    }

    const o = resp as R2ObjectBody;

    if (!o.body) {
      throw new EtagMismatch();
    }

    const a = await o.arrayBuffer();
    return {
      data: a,
      etag: o.etag,
      cacheControl: o.httpMetadata?.cacheControl,
      expires: o.httpMetadata?.cacheExpiry?.toISOString(),
    };
  }
}

export default {
  async fetch(
    request: Request,
    env: Env,
    ctx: ExecutionContext
  ): Promise<Response> {

    const url = new URL(request.url);

    // if path is a single world (string with underscores)
    // get all available pmtiles for that world
    if (url.pathname.match(/^\/list\/?$/) !== null) {
      // console.log(`Getting available pmtiles for world: ${worldName}`);
      const pmtiles = await getAvailablePmTilesFromD1(
        env.OCAP2_DATA
      );
      return new Response(
        // return pmtiles (json object)
        JSON.stringify(pmtiles),
        {
          status: 200, headers: {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "public, max-age=86400"
          }
        }
      );
    }


    if (request.method.toUpperCase() === "POST")
      return new Response(undefined, { status: 405 });


    const { ok, name, tile, ext } = tile_path(url.pathname);

    const cache = caches.default;

    let allowedOrigin = "";
    if (typeof env.ALLOWED_ORIGINS !== "undefined") {
      for (const o of env.ALLOWED_ORIGINS.split(",")) {
        if (o === request.headers.get("Origin") || o === "*") {
          allowedOrigin = o;
        }
      }
    }

    const cached = await cache.match(request.url);
    if (cached) {
      const respHeaders = new Headers(cached.headers);
      if (allowedOrigin)
        respHeaders.set("Access-Control-Allow-Origin", allowedOrigin);
      respHeaders.set("Vary", "Origin");

      return new Response(cached.body, {
        headers: respHeaders,
        status: cached.status,
      });
    }

    const cacheableResponse = (
      body: ArrayBuffer | string | undefined,
      cacheableHeaders: Headers,
      status: number
    ) => {
      cacheableHeaders.set(
        "Cache-Control",
        env.CACHE_CONTROL || "public, max-age=86400"
      );

      const cacheable = new Response(body, {
        headers: cacheableHeaders,
        status: status,
      });

      ctx.waitUntil(cache.put(request.url, cacheable));

      const respHeaders = new Headers(cacheableHeaders);
      if (allowedOrigin)
        respHeaders.set("Access-Control-Allow-Origin", allowedOrigin);
      respHeaders.set("Vary", "Origin");
      return new Response(body, { headers: respHeaders, status: status });
    };

    const cacheableHeaders = new Headers();


    // static serve any png or json files
    if (["GET", "HEAD", "OPTIONS"].includes(request.method) &&
      url.pathname.startsWith("/fonts")
    ) {
      const objectKey = url.pathname.substring(1);
      console.log('objectKey', objectKey);
      return await checkFonts(env, objectKey, cacheableHeaders, cacheableResponse);
    }
    if (["GET", "HEAD", "OPTIONS"].includes(request.method) &&
      url.pathname.startsWith("/styles")
    ) {
      const objectKey = url.pathname.substring(1);
      console.log('objectKey', objectKey);
      // fetch from r2
      // console.log(url.pathname);
      const resp = await env.BUCKET.get(objectKey);
      if (!resp) {
        return cacheableResponse("File not found", cacheableHeaders, 404);
      }

      const o = resp as R2ObjectBody;
      const a = await o.arrayBuffer();
      const objectExtension = objectKey.split('.').pop();
      let contentType
      switch (objectExtension) {
        case "png":
          contentType = "image/png";
          break;
        case "json":
          contentType = "application/json";
          break;
        default:
          contentType = "application/octet-stream";
      }
      cacheableHeaders.set("Content-Type", contentType);
      return cacheableResponse(a, cacheableHeaders, 200);
    }


    if (!ok) {
      return new Response("Invalid URL", { status: 404 });
    }

    const source = new R2Source(env, name);
    const p = new PMTiles(source, CACHE, nativeDecompress);
    try {
      const pHeader = await p.getHeader();

      if (!tile) {
        cacheableHeaders.set("Content-Type", "application/json");
        const t = await p.getTileJson(
          `https://${env.PUBLIC_HOSTNAME || url.hostname}/${name}`
        );
        return cacheableResponse(JSON.stringify(t), cacheableHeaders, 200);
      }

      if (tile[0] < pHeader.minZoom || tile[0] > pHeader.maxZoom) {
        return cacheableResponse(undefined, cacheableHeaders, 404);
      }

      for (const pair of [
        [TileType.Mvt, "mvt"],
        [TileType.Png, "png"],
        [TileType.Jpeg, "jpg"],
        [TileType.Webp, "webp"],
        [TileType.Avif, "avif"],
      ]) {
        if (pHeader.tileType === pair[0] && ext !== pair[1]) {
          if (pHeader.tileType === TileType.Mvt && ext === "pbf") {
            // allow this for now. Eventually we will delete this in favor of .mvt
            continue;
          }
          return cacheableResponse(
            `Bad request: requested .${ext} but archive has type .${pair[1]}`,
            cacheableHeaders,
            400
          );
        }
      }

      const tiledata = await p.getZxy(tile[0], tile[1], tile[2]);

      switch (pHeader.tileType) {
        case TileType.Mvt:
          cacheableHeaders.set("Content-Type", "application/x-protobuf");
          break;
        case TileType.Png:
          cacheableHeaders.set("Content-Type", "image/png");
          break;
        case TileType.Jpeg:
          cacheableHeaders.set("Content-Type", "image/jpeg");
          break;
        case TileType.Webp:
          cacheableHeaders.set("Content-Type", "image/webp");
          break;
      }

      if (tiledata) {
        return cacheableResponse(tiledata.data, cacheableHeaders, 200);
      }
      return cacheableResponse(undefined, cacheableHeaders, 204);
    } catch (e) {
      if (e instanceof KeyNotFoundError) {
        return cacheableResponse("Archive not found", cacheableHeaders, 404);
      }
      throw e;
    }
  },
};


type PMTilesD1Record = {
  worldName: string;
  displayName: string;
  mapJson: string;
  layerKeys: string;
  lastUpdated: string;
};
async function getAvailablePmTilesFromD1(db: D1Database):
  Promise<Record<string, object>> {

  const data = await db.prepare(`SELECT * FROM pmtiles_data ORDER BY worldName`)
    .all<PMTilesD1Record>();

  if (!data) {
    return Promise.resolve({});
  }

  const results = <Record<string, object>>{};

  data.results.forEach((record) => {
    results[record.worldName] = {
      worldName: record.worldName,
      displayName: record.displayName,
      mapJson: JSON.parse(record.mapJson),
      layerKeys: JSON.parse(record.layerKeys),
      lastUpdated: record.lastUpdated
    };
  });

  return Promise.resolve(results);

}
async function checkFonts(env: Env, objectKey: string, cacheableHeaders: Headers, cacheableResponse: any) {
  // split the path into parts
  // fonts/Noto%20Sans%20Bold,Noto%20Sans%20Ragular/0-255.pbf
  const newKey = objectKey.replace(/%20/g, ' ');
  const parts = newKey.split('/');
  const fonts = parts[1].split(',');
  for (let i = 0; i < fonts.length; i++) {
    const font = fonts[i];
    const newObjectKey = `fonts/${font}/${parts[2]}`;
    console.log('newObjectKey', newObjectKey);
    const resp = await env.BUCKET.get(newObjectKey);
    if (!resp) {
      continue;
    }
    const o = resp as R2ObjectBody;
    const a = await o.arrayBuffer();
    cacheableHeaders.set("Content-Type", "application/x-protobuf");
    return cacheableResponse(a, cacheableHeaders, 200);
  }
  return new Response("Font not found", { status: 404 });
}
