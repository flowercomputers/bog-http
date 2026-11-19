// Compiled against types generated from tests/golden/openapi.json by
// scripts/check-openapi.sh: the document must be usable by a real client.
import type { paths, components } from "./api";
type Page = paths["/customers"]["get"]["responses"]["200"]["content"]["application/json"];
const p: Page = { epoch: 3, data: { items: [{ key: 1, value: { name: "a", region: "eu" } }], next: null } };
type Insert = paths["/customers"]["post"]["requestBody"]["content"]["application/json"];
const c: Insert = { name: "ada", region: "eu" };
type Hits = paths["/names"]["get"]["responses"]["200"]["content"]["application/json"]["data"];
const h: Hits = [{ key: 7, score: 1.5 }];
type Prob = components["schemas"]["Problem"];
const e: Prob = { type: "rejected", title: "Write rejected", status: 422, at: "orders" };
type Q = paths["/ranked"]["get"]["parameters"]["query"];
const q: Q = { by: "value", order: "desc", limit: 5 };
// @ts-expect-error: `by` is an enum
const bad: Q = { by: "price" };
type TxBody = paths["/_tx"]["post"]["requestBody"]["content"]["application/json"];
const t: TxBody = [
  { resource: "prefs", op: "upsert", key: "theme", body: 1 },
  { resource: "orders", op: "append", body: { customer: 1, cents: 5 } },
];
// @ts-expect-error: an op the resource doesn't have
const badTx: TxBody = [{ resource: "orders", op: "upsert", key: 1, body: { customer: 1, cents: 5 } }];
type Hdr = NonNullable<paths["/prefs/{key}"]["get"]["parameters"]["header"]>;
const hdr: Hdr = { "bog-min-epoch": 7 };
export { p, c, h, e, q, bad, t, badTx, hdr };
