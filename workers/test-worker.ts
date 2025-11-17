import type { Job } from "../src/jobs";
import { BaseWorker } from "../src/base-worker";

export class TestWorker extends BaseWorker {
  protected async process(job: Job): Promise<void> {
    // For testing, just process the job - no need to return data
    JSON.parse(job.data);
  }
}

new TestWorker().start();
