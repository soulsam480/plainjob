import { BaseWorker } from "../../src/base-worker";
import type { Job } from "../../src/jobs";

class ScheduledWorker extends BaseWorker {
  protected async process(job: Job): Promise<any> {
    // Worker for scheduled jobs, just returns empty object
    return { scheduled: true };
  }
}

new ScheduledWorker().start();