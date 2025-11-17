import type { Logger, Queue } from "../src/plainjob";
import {
  defineQueue,
  defineWorker,
  JobStatus,
  processAll,
} from "../src/plainjob";
import { type Connection, setupQueueDeps } from "../src/queue";

const logger: Logger = {
  error: console.error,
  warn: console.warn,
  info: () => {},
  debug: () => {},
};

async function queueJobs(queue: Queue, count: number) {
  const jobs = [];

  for (let i = 0; i < count; i++) {
    jobs.push({ jobId: i });
  }

  await queue.addMany("bench", jobs);
}

export async function runScenario(
  connection: Connection,
  jobCount: number,
  concurrency: number,
) {
  console.log(
    `running scenario - jobs: ${jobCount}, concurrency: ${concurrency}`,
  );

  await connection.exec(
    "DROP INDEX IF EXISTS idx_jobs_status_type_next_run_at",
  );

  await connection.exec(
    "DROP INDEX IF EXISTS idx_scheduled_jobs_status_type_next_run_at",
  );

  await connection.exec("DROP TABLE IF EXISTS plainjob_jobs");
  await connection.exec("DROP TABLE IF EXISTS  plainjob_scheduled_jobs");

  await setupQueueDeps(connection);

  const queue = defineQueue({ connection, logger });

  await queueJobs(queue, jobCount);

  const start = Date.now();

  const workerPromises: Promise<void>[] = [];

  const worker = defineWorker(
    "bench",
    new URL("./bench-worker.ts", import.meta.url).toString(),
    {
      queue,
      logger,
      concurrency: concurrency,
    },
  );

  workerPromises.push(
    processAll(queue, worker, { logger, timeout: 60 * 1000 }),
  );

  await Promise.all(workerPromises);

  if ((await queue.countJobs({ status: JobStatus.Pending })) > 0) {
    throw new Error(
      `pending jobs remaining: ${await queue.countJobs({
        status: JobStatus.Pending,
      })}`,
    );
  }

  if ((await queue.countJobs({ status: JobStatus.Processing })) > 0) {
    throw new Error(
      `processing jobs remaining: ${queue.countJobs({
        status: JobStatus.Processing,
      })}`,
    );
  }

  await queue.close();

  const elapsed = Date.now() - start;
  const jobsPerSecond = jobCount / (elapsed / 1000);

  console.log(`jobs: ${jobCount}`);
  console.log(`parallel workers: ${concurrency}`);
  console.log(`time elapsed: ${elapsed} ms`);
  console.log(`jobs/second: ${jobsPerSecond.toFixed(2)}`);
  console.log("------------------------");

  return jobsPerSecond;
}
