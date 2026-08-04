import { pumpJobs } from "./istari-job-pump.mjs";

export async function runBackgroundPump(fetchImpl = fetch, jobId = "") {
  return pumpJobs(fetchImpl, jobId);
}

export default async (request) => {
  const body = await request.json().catch(() => ({}));
  await runBackgroundPump(fetch, body.job_id);
};

export const config = {
  background: true,
};
