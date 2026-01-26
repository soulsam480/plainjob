import { BaseWorker } from "../../src/base-worker";
import type { Job } from "../../src/jobs";

class TestWorker extends BaseWorker {
  protected async process(job: Job): Promise<any> {
    // Simple test worker that just returns the job data
    const data = JSON.parse(job.data);
    return { processed: true, ...data };
  }
}

new TestWorker().start();