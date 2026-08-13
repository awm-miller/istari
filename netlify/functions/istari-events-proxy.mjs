import { stream } from "@netlify/functions";

export const handler = stream(async (event) => {
  const origin = String(process.env.ISTARI_BACKEND_ORIGIN || "").replace(/\/$/, "");
  const token = String(process.env.ISTARI_BACKEND_PROXY_TOKEN || "");
  const jobId = String(event.queryStringParameters?.job || "");
  if (!origin || !token || !/^[a-f0-9]+$/.test(jobId)) return Response.json({ ok: false, error: "Invalid event stream." }, { status: 400 });
  const upstream = await fetch(`${origin}/api/investigations/${jobId}/events`, {
    headers: { Accept: "text/event-stream", "X-Istari-Proxy-Token": token },
  });
  return new Response(upstream.body, {
    status: upstream.status,
    headers: { "content-type": "text/event-stream", "cache-control": "no-cache, no-transform" },
  });
});
