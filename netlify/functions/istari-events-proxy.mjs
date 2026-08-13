import { stream } from "@netlify/functions";

export const handler = stream(async (event) => {
  const origin = String(process.env.ISTARI_BACKEND_ORIGIN || "").replace(/\/$/, "");
  const token = String(process.env.ISTARI_BACKEND_PROXY_TOKEN || "");
  const jobId = String(event.queryStringParameters?.job || "");
  if (!origin || !token || !/^[a-f0-9]+$/.test(jobId)) {
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
