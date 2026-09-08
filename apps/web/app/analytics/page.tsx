"use client";

import { TimeSeries } from "@/components/charts";
import { PageHeader } from "@/components/shell";
import { Card, EmptyState, Spinner } from "@/components/ui";
import { money, num } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import { useState } from "react";

const RANGES = [
  { label: "1h", minutes: 60 },
  { label: "6h", minutes: 360 },
  { label: "24h", minutes: 1440 },
];

function Chart({ metric, label, minutes, kind }: { metric: string; label: string; minutes: number; kind?: "pct" | "money" }) {
  const { data } = useApi<{ points: { t: string; value: number }[] }>(
    `/api/v1/analytics/timeseries?metric=${metric}`,
    { intervalMs: 15000, deps: [minutes] },
  );
  const pts = (data?.points ?? []).slice(-minutes);
  return (
    <Card title={label}>
      {pts.length === 0 ? (
        <EmptyState>No data yet.</EmptyState>
      ) : (
        <TimeSeries
          data={pts}
          series={[{ key: "value", label }]}
          area
          yFormat={
            kind === "pct" ? (v) => `${(v * 100).toFixed(0)}%` : kind === "money" ? (v) => `$${num(v)}` : undefined
          }
        />
      )}
    </Card>
  );
}

function TopList({ metric, label, minutes }: { metric: string; label: string; minutes: number }) {
  const { data } = useApi<{ top: { key: string; score: number }[] }>(
    `/api/v1/analytics/top?metric=${metric}&n=10&minutes=${minutes}`,
    { intervalMs: 15000, deps: [minutes] },
  );
  return (
    <Card title={label}>
      {!data || data.top.length === 0 ? (
        <EmptyState>No data.</EmptyState>
      ) : (
        <ol className="space-y-1 text-sm">
          {data.top.map((t, i) => (
            <li key={t.key} className="flex justify-between border-b border-border/50 py-1">
              <span className="text-muted">{i + 1}. {t.key}</span>
              <span className="tabular font-medium">{num(t.score, 1)}</span>
            </li>
          ))}
        </ol>
      )}
    </Card>
  );
}

export default function AnalyticsPage() {
  const [minutes, setMinutes] = useState(60);
  return (
    <div>
      <PageHeader
        title="Analytics"
        desc="Windowed aggregations computed by the analytics stream processor (event-time, 1-minute tumbling)."
        right={
          <div className="flex gap-1">
            {RANGES.map((r) => (
              <button
                key={r.minutes}
                onClick={() => setMinutes(r.minutes)}
                className={`rounded border px-2 py-1 text-xs ${
                  minutes === r.minutes ? "border-accent text-accent" : "border-border text-muted"
                }`}
              >
                {r.label}
              </button>
            ))}
          </div>
        }
      />
      <div className="grid gap-3 lg:grid-cols-2">
        <Chart metric="events_per_sec" label="Events / sec" minutes={minutes} />
        <Chart metric="revenue" label="Revenue / min" minutes={minutes} kind="money" />
        <Chart metric="orders_created" label="Orders / min" minutes={minutes} />
        <Chart metric="order_cancellation_rate" label="Cancellation rate" minutes={minutes} kind="pct" />
        <Chart metric="payment_failure_rate" label="Payment failure rate" minutes={minutes} kind="pct" />
        <Chart metric="active_users" label="Active users / min" minutes={minutes} />
        <Chart metric="conversion_rate" label="Conversion rate" minutes={minutes} kind="pct" />
        <Chart metric="avg_order_value" label="Average order value" minutes={minutes} kind="money" />
      </div>
      <div className="mt-3 grid gap-3 lg:grid-cols-3">
        <TopList metric="top_products" label="Top products (by value)" minutes={minutes} />
        <TopList metric="events_by_region" label="Top regions (by volume)" minutes={minutes} />
        <TopList metric="top_payment_failure_reasons" label="Top failure reasons" minutes={minutes} />
      </div>
    </div>
  );
}
