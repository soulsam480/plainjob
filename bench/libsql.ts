import { libsql } from "../src/queue";
import { runScenario } from "./run-scenario";
import Database from "libsql";

function createDb() {
  const db = new Database("libsql://test-soulsam480.aws-ap-south-1.turso.io", {
    authToken:
      "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicnciLCJpYXQiOjE3NjI2OTg4NDEsImlkIjoiYmI1NDg2MjAtNThmZS00NDJlLWIxMjUtOGRjODE5MTg0MjdjIiwicmlkIjoiN2IyODMwYTktOWViZi00MWQyLWEyNWItZWFjMDc5NjQxNTI1In0.xZ09dUP2IGfFs99OjWtz_rfj81GuEttyEZiljUxpnlBaMLLKJf9t5c0IAXbPktzu4TnebZ4rSiTWtIm9q_-VBw",
  });

  return Object.assign(db, { name: "bench.db" });
}

async function runScenarios() {
  await runScenario(libsql(createDb()), 32000, 0, 1);
  await runScenario(libsql(createDb()), 32000, 0, 2);
  await runScenario(libsql(createDb()), 32000, 0, 4);
}

runScenarios().catch(console.error);
