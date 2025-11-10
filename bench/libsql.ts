import { createClient } from "@libsql/client";
import { libsql } from "../src/queue";
import { runScenario } from "./run-scenario";

export function createDb() {
  return createClient({ url: ":memory:" });
}

async function runScenarios() {
  await runScenario(libsql(createDb()), 32000, 0, 1);
  await runScenario(libsql(createDb()), 32000, 0, 2);
  await runScenario(libsql(createDb()), 32000, 0, 4);
}

runScenarios().catch(console.error);
