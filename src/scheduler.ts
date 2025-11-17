import { cpus } from "node:os";
import cronParser from "cron-parser";
import type { Job, Logger } from "./jobs";
import type { Queue } from "./queue";

export interface SchedulerOptions {
  queue: Queue;
  jobType: string;
  workerPath: string;
  pollInterval?: number;
  logger?: Logger;
  onCompleted?: (job: Job) => void;
  onFailed?: (job: Job, error: string) => void;
  onProcessing?: (job: Job) => void;
  // TODO: implement concurrency
  concurrency?: number;
}

export class JobScheduler {
  private queue: Queue;
  private jobType: string;
  private workerPath: string;
  private logger: Logger;
  private pollInterval: number;
  private onCompleted?: (job: Job) => void;
  private onFailed?: (job: Job, error: string) => void;
  private onProcessing?: (job: Job) => void;
  private concurrency: number;
  private shouldKeepRunning = false;
  private schedulerId: string;
  private activeJobs = new Set<string>();
  private worker?: Worker;

  constructor(options: SchedulerOptions) {
    this.queue = options.queue;
    this.jobType = options.jobType;
    this.workerPath = options.workerPath;
    this.logger = options.logger || console;
    this.pollInterval = options.pollInterval ?? 1000;
    this.onCompleted = options.onCompleted;
    this.onFailed = options.onFailed;
    this.onProcessing = options.onProcessing;
    this.concurrency = options.concurrency ?? cpus().length;
    this.schedulerId = Math.random().toString(36).substring(2, 15);
  }

  async start(): Promise<void> {
    this.shouldKeepRunning = true;

    this.logger.info(
      `scheduler [${this.schedulerId}] starting for job type '${this.jobType}' with ${this.concurrency} concurrent workers`,
    );

    const promises: Promise<void>[] = [];

    promises.push(this.processJobs());

    promises.push(this.processScheduledJobs());

    await Promise.all(promises);
    this.logger.info(`scheduler [${this.schedulerId}] stopped`);
  }

  async stop(): Promise<void> {
    this.logger.info(`scheduler [${this.schedulerId}] shutting down...`);
    this.worker?.terminate();
    this.shouldKeepRunning = false;
    this.logger.info(`scheduler [${this.schedulerId}] shut down complete`);
  }

  private async processJobs(): Promise<void> {
    while (this.shouldKeepRunning) {
      const jobId = await this.queue.getAndMarkJobAsProcessing(this.jobType);

      if (!jobId) {
        await this.sleep(this.pollInterval);
        continue;
      }

      const job = await this.queue.getJobById(jobId.id);
      if (!job) continue;

      const jobKey = `${job.type}-${job.id}`;

      if (this.activeJobs.has(jobKey)) {
        continue;
      }

      this.activeJobs.add(jobKey);

      this.processJob(job).finally(() => {
        this.activeJobs.delete(jobKey);
      });
    }
  }

  private async processScheduledJobs(): Promise<void> {
    while (this.shouldKeepRunning) {
      const scheduledJob =
        await this.queue.getAndMarkScheduledJobAsProcessing();

      if (!scheduledJob) {
        await this.sleep(this.pollInterval);
        continue;
      }

      this.logger.debug(
        `processing scheduled job '${scheduledJob.id}' '${scheduledJob.type}'`,
      );

      const nextRunAt = cronParser
        .parseExpression(scheduledJob.cronExpression)
        .next()
        .toDate()
        .getTime();

      await this.queue.markScheduledJobAsIdle(scheduledJob.id, nextRunAt);
      await this.queue.add(scheduledJob.type, {});

      this.logger.debug(
        `adding job '${scheduledJob.id}' '${scheduledJob.type}' from scheduled job`,
      );
    }
  }

  private async processJob(job: Job): Promise<void> {
    this.onProcessing?.(job);
    this.logger.debug(`processing job ${job.id}, ${job.type}, ${job.data}`);

    const worker = this.spawnOrGetWorker();

    return new Promise<void>((resolve, reject) => {
      const handleMessage = (event: MessageEvent) => {
        switch (event.data.type) {
          case "JOB_COMPLETE":
            {
              this.queue.markJobAsDone(job.id);
              this.onCompleted?.(job);

              this.logger.debug(`marking job ${job.id} as 'done'`);

              worker.removeEventListener("message", handleMessage);

              resolve();
            }
            break;

          case "JOB_FAILED":
            {
              this.queue.markJobAsFailed(job.id, event.data.error);
              this.onFailed?.(job, event.data.error);

              this.logger.error(`marking job ${job.id} as 'failed'`);

              worker.removeEventListener("message", handleMessage);

              resolve();
            }
            break;

          default:
        }
      };

      const handleError = (error: ErrorEvent) => {
        this.logger.error(`Worker error: ${error.message || error}`);

        worker.removeEventListener("error", handleError);

        reject(new Error(`Worker error: ${error.message || error}`));
      };

      worker.addEventListener("message", handleMessage);
      worker.addEventListener("error", handleError);

      worker.postMessage({
        type: "PROCESS_JOB",
        jobId: job.id,
        jobType: job.type,
        jobData: job.data,
      });
    });
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private spawnOrGetWorker() {
    this.worker ??= new Worker(this.workerPath);
    return this.worker;
  }
}
