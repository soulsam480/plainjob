import { type Job, JobStatus, type Logger } from "./jobs";
import type { Queue } from "./queue";
import { JobScheduler, type SchedulerOptions } from "./scheduler";

export type Worker = {
  /** Start the worker, processing jobs one by one. Returns a promise that resolves when the worker is stopped. */
  start: () => Promise<void>;
  /** Stop the worker gracefully, waiting for the current job to finish. */
  stop: () => Promise<void>;
};

type WorkerOptions = {
  /** The queue instance to process jobs from. */
  queue: Queue;
  /** Interval in milliseconds to poll for new jobs (default: 1000). */
  pollInterval?: number;
  /** Logger instance for the worker (default: console). */
  logger?: Logger;
  /** Callback function called when a job is completed successfully. */
  onCompleted?: (job: Job) => void;
  /** Callback function called when a job fails, providing the error message. */
  onFailed?: (job: Job, error: string) => void;
  /** Callback function called when a job starts processing. */
  onProcessing?: (job: Job) => void;
  /** Number of worker threads to spawn (default: number of CPU cores). */
  concurrency?: number;
};

export function defineWorker(
  jobType: string,
  workerPath: string,
  options: WorkerOptions,
): Worker {
  // Create scheduler options
  const schedulerOptions: SchedulerOptions = {
    queue: options.queue,
    jobType,
    workerPath,
    pollInterval: options.pollInterval,
    logger: options.logger,
    onCompleted: options.onCompleted,
    onFailed: options.onFailed,
    onProcessing: options.onProcessing,
    concurrency: options.concurrency,
  };

  // Create job scheduler
  const scheduler = new JobScheduler(schedulerOptions);

  return {
    async start(): Promise<void> {
      await scheduler.start();
    },
    async stop(): Promise<void> {
      await scheduler.stop();
    },
  };
}

/**
 * Start a worker and wait for all jobs to be processed before shutting worker down.
 * This will hang until timeout of worker is not processing all job types in queue. */
export async function processAll(
  queue: Queue,
  worker: Worker,
  opts?: { logger?: Logger; timeout?: number },
) {
  const log = opts?.logger || console;
  const timeout = opts?.timeout ?? 1000;

  void worker.start();

  const start = Date.now();

  await new Promise((resolve) => setTimeout(resolve, 10));

  while (
    (await queue.countJobs({ status: JobStatus.Pending })) +
      (await queue.countJobs({ status: JobStatus.Processing })) >
    0
  ) {
    log.debug("waiting for jobs to be processed");

    await new Promise((resolve) => setTimeout(resolve, 50));

    if (Date.now() - start > timeout) {
      throw new Error("timeout while waiting for all jobs to be processed");
    }
  }

  log.debug("all processed, shutting down worker");

  await worker.stop();
}
