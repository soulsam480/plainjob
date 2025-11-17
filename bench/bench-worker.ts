import type { Job } from "../src/jobs";
import { BaseWorker } from "../src/base-worker";

export class BenchWorker extends BaseWorker {
  protected async process(job: Job): Promise<void> {
    // Simulate minimal work for benchmarking
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
}

new BenchWorker().start();
