import { BaseWorker } from "../../src/base-worker";
import type { Job } from "../../src/jobs";

class BenchWorker extends BaseWorker {
  protected async process(job: Job): Promise<any> {
    // Simple benchmark worker that just resolves
    return { processed: true };
  }
}

new BenchWorker().start();