import type { Expect } from "bun:test";
import Database from "libsql";
import { beforeEach, describe, expect, it } from "vitest";
import { libsql } from "../src/queue";
import { getTestSuite } from "./test-suite";

getTestSuite(describe, beforeEach, it, expect as unknown as Expect, () => {
  return libsql(
    // new Database("libsql://test-soulsam480.aws-ap-south-1.turso.io", {
    //   authToken:
    //     "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicnciLCJpYXQiOjE3NjI2OTg4NDEsImlkIjoiYmI1NDg2MjAtNThmZS00NDJlLWIxMjUtOGRjODE5MTg0MjdjIiwicmlkIjoiN2IyODMwYTktOWViZi00MWQyLWEyNWItZWFjMDc5NjQxNTI1In0.xZ09dUP2IGfFs99OjWtz_rfj81GuEttyEZiljUxpnlBaMLLKJf9t5c0IAXbPktzu4TnebZ4rSiTWtIm9q_-VBw",
    // }),
    new Database(":memory:"),
  );
});
