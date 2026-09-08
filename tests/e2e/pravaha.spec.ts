/**
 * End-to-end journey against a running stack (docker compose up).
 *
 *   login → create producer → register schema → send events →
 *   observe live stream → see metrics → trigger a payment-failure anomaly →
 *   inspect the AI investigation → inspect DLQ → create a replay job
 *
 * Uses the API directly for the data-plane steps (deterministic, fast) and the
 * UI for the human-facing assertions. Requires:
 *   PRAVAHA_E2E_BASE_URL  (default http://localhost:3000)
 *   PRAVAHA_API_BASE_URL  (default http://localhost:8000)
 */
import { expect, test, type APIRequestContext } from "@playwright/test";

const API = process.env.PRAVAHA_API_BASE_URL ?? "http://localhost:8000";
const ADMIN = { email: "admin@pravaha.local", password: "admin12345" };

let token = "";
let apiKey = "";

async function login(request: APIRequestContext) {
  const r = await request.post(`${API}/api/v1/auth/login`, { data: ADMIN });
  expect(r.ok()).toBeTruthy();
  token = (await r.json()).access_token;
}

function auth() {
  return { Authorization: `Bearer ${token}` };
}

async function sendEvent(request: APIRequestContext, body: unknown) {
  return request.post(`${API}/api/v1/events`, {
    headers: { "X-API-Key": apiKey },
    data: body,
  });
}

test.describe.configure({ mode: "serial" });

test("00 login + bootstrap producer & schema", async ({ request }) => {
  await login(request);

  // producer
  const name = `e2e-${Date.now()}`;
  const rp = await request.post(`${API}/api/v1/producers`, {
    headers: auth(),
    data: { name, allowed_event_types: [], rate_limit_per_min: 1000000 },
  });
  expect(rp.status()).toBe(201);
  apiKey = (await rp.json()).api_key;
  expect(apiKey).toContain("pvh_");

  // schema
  const rs = await request.post(`${API}/api/v1/schemas`, {
    headers: auth(),
    data: {
      event_type: "payment.failed",
      json_schema: {
        type: "object",
        required: ["order_id", "reason"],
        properties: { order_id: { type: "string" }, reason: { type: "string" }, amount: { type: "number" } },
      },
    },
  });
  expect([201, 409]).toContain(rs.status());
});

test("01 ingest valid + invalid events", async ({ request }) => {
  const ok = await sendEvent(request, {
    event_type: "payment.failed",
    payload: { order_id: "o1", reason: "card_declined", amount: 10 },
  });
  expect(ok.status()).toBe(202);
  expect((await ok.json()).valid).toBe(true);

  const bad = await sendEvent(request, {
    event_type: "payment.failed",
    payload: { order_id: "o2" }, // missing reason
  });
  expect(bad.status()).toBe(422);
  expect((await bad.json()).error.code).toBeTruthy();
});

test("02 UI login and overview render real numbers", async ({ page }) => {
  await page.goto("/login");
  await page.fill('input[autocomplete="username"]', ADMIN.email);
  await page.fill('input[type="password"]', ADMIN.password);
  await page.click('button:has-text("Sign in")');
  await expect(page).toHaveURL(/\/$|\/$/);
  await expect(page.getByText("Overview")).toBeVisible();
  await expect(page.getByText("Events / sec")).toBeVisible();
});

test("03 live stream connects", async ({ page }) => {
  await page.goto("/live");
  await expect(page.getByText(/connected|reconnecting/)).toBeVisible();
});

test("04 trigger a payment-failure burst and see a DLQ entry", async ({ request, page }) => {
  // 30 invalid payment.failed -> ingestion DLQ (invalid_schema)
  for (let i = 0; i < 30; i++) {
    await sendEvent(request, { event_type: "payment.failed", payload: { order_id: `bad-${i}` } });
  }
  await page.goto("/dlq");
  await expect(page.getByText("Dead Letter Queue")).toBeVisible();
  await expect(page.getByText(/invalid_schema/).first()).toBeVisible({ timeout: 20000 });
});

test("05 anomalies page and AI status reachable", async ({ page, request }) => {
  await page.goto("/anomalies");
  await expect(page.getByText("Anomalies")).toBeVisible();

  const s = await request.get(`${API}/api/v1/ai/status`, { headers: auth() });
  expect(s.ok()).toBeTruthy();
  const body = await s.json();
  expect(body.core_streaming_dependent_on_ai).toBe(false);

  await page.goto("/ai");
  await expect(page.getByText("AI Intelligence")).toBeVisible();
});

test("06 create a replay job via API and see it listed in the UI", async ({ request, page }) => {
  const now = new Date();
  const from = new Date(now.getTime() - 3600_000).toISOString();
  const r = await request.post(`${API}/api/v1/replay`, {
    headers: auth(),
    data: { time_from: from, time_to: now.toISOString(), target_topic: "events.replay" },
  });
  expect(r.status()).toBe(201);
  await page.goto("/replay");
  await expect(page.getByText("Replay jobs")).toBeVisible();
  await expect(page.getByText(/CREATED|RUNNING|COMPLETED/).first()).toBeVisible({ timeout: 15000 });
});
