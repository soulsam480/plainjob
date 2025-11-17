import type { Job } from "../src/jobs";
import { BaseWorker } from "../src/base-worker";

export class PaintErrorWorker extends BaseWorker {
  protected async process(job: Job): Promise<void> {
    throw new Error("test error");
  }
}

new PaintErrorWorker().start();
