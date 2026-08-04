export async function pumpJobs(fetchImpl = fetch, jobId = "") {
  const origin = String(process.env.ISTARI_SITES_ORIGIN || "").replace(/\/$/, "");
  const token = String(process.env.ISTARI_SITES_PROXY_TOKEN || "");
  if (!origin || !token) throw new Error("Istari discovery service is not configured.");
  const safeJobId = /^[a-f0-9]+$/.test(String(jobId)) ? String(jobId) : "";
  const target = safeJobId ? `/api/internal/pump/${safeJobId}` : "/api/internal/pump";
  const response = await fetchImpl(`${origin}${target}`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "X-Istari-Proxy-Token": token,
    },
  });
  if (!response.ok) throw new Error(`Istari job pump failed with ${response.status}.`);
  return response.json();
}

export default async () => {
  await pumpJobs();
};

export const config = {
  schedule: "* * * * *",
};
