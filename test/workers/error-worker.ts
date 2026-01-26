import { BaseWorker } from "../../src/base-worker";
import type { Job } from "../../src/jobs";

class ErrorWorker extends BaseWorker {
  protected async process(job: Job): Promise<any> {
    throw new Error("test error");
  }
}

new ErrorWorker().start();
