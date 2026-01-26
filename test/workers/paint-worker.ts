import { BaseWorker } from "../../src/base-worker";
import type { Job } from "../../src/jobs";

class PaintWorker extends BaseWorker {
  protected async process(job: Job): Promise<any> {
    const data = JSON.parse(job.data);
    return { painted: true, color: data.color };
  }
}

new PaintWorker().start();