import type { Job, Logger } from "../src/plainjob";
import { defineQueue, defineWorker } from "../src/plainjob";
import { libsql, setupQueueDeps } from "../src/queue";
import { processAll } from "../src/worker";
import { createDb } from "./libsql";

const logger: Logger = {
  error: console.error,
  warn: console.warn,
  info: () => {},
  debug: () => {},
};

async function run() {
  const connection = libsql(createDb());

  await setupQueueDeps(connection);

  const queue = defineQueue({ connection, logger });

  const worker = defineWorker("bench", async (_job: Job) => Promise.resolve(), {
    queue,
    logger,
  });

  await processAll(queue, worker, { logger, timeout: 60 * 1000 });

  await queue.close();
  process.exit(0);
}

run().catch((error) => {
  console.error(error);
  process.exit(1);
});
