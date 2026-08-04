import { pumpJobs } from "./istari-job-pump.mjs";

export async function runBackgroundPump(fetchImpl = fetch) {
  return pumpJobs(fetchImpl);
}

export default async () => {
  await runBackgroundPump();
};

export const config = {
  background: true,
};
