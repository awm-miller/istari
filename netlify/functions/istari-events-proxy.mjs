import { stream } from "@netlify/functions";

export const handler = stream(async (event) => {
  const origin = String(process.env.ISTARI_BACKEND_ORIGIN || "").replace(/\/$/, "");
  const token = String(process.env.ISTARI_BACKEND_PROXY_TOKEN || "");
  if (!origin || !token) {
    return {
      statusCode: 503,
      headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" },
      body: JSON.stringify({ ok: false, error: "Istari discovery service is not configured." }),
    };
  }
  const originalUrl = String(event.rawUrl || event.url || "");
  const originalPath = originalUrl ? new URL(originalUrl).pathname : String(event.path || "");
  const pathJob = originalPath.match(/^\/api\/investigations\/([a-f0-9]+)\/events$/)?.[1] || "";
  const queryJob = event.queryStringParameters?.job
    || (originalUrl ? new URL(originalUrl).searchParams.get("job") : "");
  const jobId = String(pathJob || queryJob || "");
  if (!/^[a-f0-9]+$/.test(jobId)) {
    return {
      statusCode: 400,
      headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" },
      body: JSON.stringify({ ok: false, error: "Invalid event stream." }),
    };
  }
  const upstream = await fetch(`${origin}/api/investigations/${jobId}/events`, {
    headers: { Accept: "text/event-stream", "X-Istari-Proxy-Token": token },
  });
  return {
    statusCode: upstream.status,
    headers: { "content-type": "text/event-stream", "cache-control": "no-cache, no-transform" },
    body: upstream.body,
  };
});
