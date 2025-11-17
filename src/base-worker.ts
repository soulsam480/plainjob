import type { Job } from "./jobs";

export interface WorkerMessage {
  type: "PROCESS_JOB" | "JOB_COMPLETE" | "JOB_FAILED";
  jobId?: number;
  jobData?: string;
  jobType?: string;
  error?: string;
  data?: unknown;
}

declare var self: Worker;

/**
 * Base class for worker files. Extend this class in your worker files.
 */
export abstract class BaseWorker {
  start() {
    self.onmessage = (event: MessageEvent<WorkerMessage>) => {
      this.handleMessage(event);
    };
  }

  private async handleMessage(
    event: MessageEvent<WorkerMessage>,
  ): Promise<void> {
    const message = event.data;

    if (
      message.type === "PROCESS_JOB" &&
      message.jobId &&
      message.jobData &&
      message.jobType
    ) {
      try {
        const job: Job = {
          id: message.jobId,
          type: message.jobType,
          data: message.jobData,
        };

        // Call the abstract process method that workers must implement
        const result = await this.process(job);

        // Send completion message
        self.postMessage({
          type: "JOB_COMPLETE",
          jobId: message.jobId,
          data: result,
        });
      } catch (error) {
        const errorMessage =
          error instanceof Error
            ? `${error.stack}\n${error.message}`
            : String(error);

        self.postMessage({
          type: "JOB_FAILED",
          jobId: message.jobId,
          error: errorMessage,
        });
      }
    }
  }

  /**
   * Override this method in your worker class to process jobs.
   * @param job The job to process
   * @returns Optional data to send back to main thread
   */

  // biome-ignore lint/suspicious/noExplicitAny: this is fine
  protected abstract process(job: Job): Promise<any>;
}
