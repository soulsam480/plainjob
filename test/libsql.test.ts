import type { Expect } from "bun:test";
import { createClient } from "@libsql/client";
import { beforeEach, describe, expect, it } from "vitest";
import { libsql } from "../src/queue";
import { getTestSuite } from "./test-suite";

// getTestSuite(describe, beforeEach, it, expect as unknown as Expect, () => {
//   return libsql(createClient({ url: ":memory:" }));
// });

getTestSuite(describe, beforeEach, it, expect as unknown as Expect, () => {
  return libsql(
    createClient({
      url: process.env.VITE_PUBLIC_TURSO_PATH ?? "",
      authToken: process.env.VITE_PUBLIC_TURSO_TOKEN,
    }),
  );
});
