import { libsql } from "../src/queue";
import { createDb } from "./create-db";
import { runScenario } from "./run-scenario";

async function runScenarios() {
  console.log("=== Single Worker Performance Test ===");

  await runScenario(libsql(createDb()), 100, 2);

  console.log("=== Concurrency Test ===");

  await runScenario(libsql(createDb()), 100, 4);

  console.log("=== High Volume Test ===");

  await runScenario(libsql(createDb()), 5000, 2);
}

runScenarios()
  .catch(console.error)
  .finally(() => process.exit(0));
