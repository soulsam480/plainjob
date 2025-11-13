import { libsql } from "../src/queue";
import { createDb } from "./ceeate-db";
import { runScenario } from "./run-scenario";

async function runScenarios() {
  await runScenario(libsql(createDb()), 100, 0, 2);
  // await runScenario(libsql(createDb()), 5000, 0, 2);
  // await runScenario(libsql(createDb()), 5000, 0, 4);
}

runScenarios().catch(console.error);
