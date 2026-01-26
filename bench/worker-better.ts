import Database from "better-sqlite3";
import { defineQueue, defineWorker } from "../src/plainjob";
import type { Job, Logger } from "../src/plainjob";
import { processAll } from "../src/worker";
import { better } from "../src/queue";

const logger: Logger = {
  error: console.error,
  warn: console.warn,
  info: () => {},
  debug: () => {},
};

const filename = process.argv[2];

if (!filename) {
  console.error("invalid database url specified");
  process.exit(1);
}

const connection = better(new Database(filename));

const queue = defineQueue({ connection, logger });
const worker = defineWorker("bench", "./bench/workers/bench-worker.js", {
  queue,
  logger,
});

async function run() {
  await processAll(queue, worker, { logger, timeout: 60 * 1000 });
  queue.close();
  process.exit(0);
}

run().catch((error) => {
  console.error(error);
  process.exit(1);
});
