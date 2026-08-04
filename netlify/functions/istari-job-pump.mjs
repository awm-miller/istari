export async function pumpJobs(fetchImpl = fetch) {
  const origin = String(process.env.ISTARI_SITES_ORIGIN || "").replace(/\/$/, "");
  const token = String(process.env.ISTARI_SITES_PROXY_TOKEN || "");
  if (!origin || !token) throw new Error("Istari discovery service is not configured.");
  const response = await fetchImpl(`${origin}/api/internal/pump`, {
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
