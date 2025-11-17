import type { Job } from "../src/jobs";
import { BaseWorker } from "../src/base-worker";

export class ErrorWorker extends BaseWorker {
  protected async process(job: Job): Promise<void> {
    throw new Error("Test error");
  }
}

new ErrorWorker().start();
