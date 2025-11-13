import { createClient } from "@libsql/client";

export function createDb() {
  return createClient({ url: "file:bench.db" });
}
