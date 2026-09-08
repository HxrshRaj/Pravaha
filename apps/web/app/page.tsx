"use client";

import { TimeSeries } from "@/components/charts";
import { PageHeader } from "@/components/shell";
import {
  Badge,
  Card,
  EmptyState,
  ErrorState,
  Spinner,
  StatCard,
  severityTone,
} from "@/components/ui";
import { useApi, useSSEValue } from "@/lib/hooks";
import { ago, money, num, pct } from "@/lib/format";
import Link from "next/link";

type Overview = {
  events_per_sec: number;
  events_last_5m: number;
  events_stored_total: number;
  active_producers: number;
  consumer_lag_total: number;
  anomalies_24h: number;
  dlq_pending: number;
  ai_investigations_running: number;
};

type Summary = {
  metrics: Record<string, { latest: number; avg: number; max: number; sum: number }>;
};

export default function OverviewPage() {
  const live = useSSEValue<Overview>("/api/v1/live/metrics", "metrics");
  const poll = useApi<Overview>("/api/v1/system/overview", { intervalMs: 10000 });
  const ov = live.value ?? poll.data;

  const summary = useApi<Summary>("/api/v1/analytics/summary?minutes=60", {
    intervalMs: 15000,
  });
  const tput = useApi<{ points: { t: string; events_per_min: number }[] }>(
    "/api/v1/analytics/throughput?minutes=60",
    { intervalMs: 15000 },
  );
  const anomalies = useApi<{ items: any[] }>(
    "/api/v1/anomalies?since_minutes=1440&limit=6",
    { intervalMs: 20000 },
  );

  const failSeries = useApi<{ points: { t: string; value: number }[] }>(
    "/api/v1/analytics/timeseries?metric=payment_failure_rate",
    { intervalMs: 15000 },
  );

  if (poll.error) return <ErrorState message={poll.error.message} />;

  return (
    <div>
      <PageHeader
        title="Overview"
        desc="Live platform state — every value is derived from processed events."
        right={
          <Badge tone={live.connected ? "ok" : "default"}>
            {live.connected ? "live" : "polling"}
          </Badge>
        }
      />

      {!ov ? (
        <Spinner label="Loading overview…" />
      ) : (
        <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
          <StatCard label="Events / sec" value={num(ov.events_per_sec, 2)} />
          <StatCard label="Events (5m)" value={num(ov.events_last_5m)} />
          <StatCard label="Active producers" value={num(ov.active_producers)} />
          <StatCard
            label="Consumer lag"
            value={num(ov.consumer_lag_total)}
            tone={ov.consumer_lag_total > 5000 ? "danger" : "default"}
          />
          <StatCard label="Events stored" value={num(ov.events_stored_total)} />
          <StatCard
            label="Anomalies (24h)"
            value={num(ov.anomalies_24h)}
            tone={ov.anomalies_24h > 0 ? "warn" : "default"}
          />
          <StatCard
            label="DLQ pending"
            value={num(ov.dlq_pending)}
            tone={ov.dlq_pending > 0 ? "danger" : "default"}
          />
          <StatCard
            label="AI investigations"
            value={num(ov.ai_investigations_running)}
            sub="running now"
          />
        </div>
      )}

      <div className="mt-4 grid gap-3 lg:grid-cols-2">
        <Card title="Event throughput (events / min, last hour)">
          {tput.data && tput.data.points.length > 0 ? (
            <TimeSeries
              data={tput.data.points}
              series={[{ key: "events_per_min", label: "events/min" }]}
              area
            />
          ) : (
            <EmptyState>No throughput windows yet. Start the generator.</EmptyState>
          )}
        </Card>
        <Card title="Payment failure rate">
          {failSeries.data && failSeries.data.points.length > 0 ? (
            <TimeSeries
              data={failSeries.data.points}
              series={[{ key: "value", label: "failure rate", color: "rgb(var(--danger))" }]}
              yFormat={(v) => `${(v * 100).toFixed(0)}%`}
            />
          ) : (
            <EmptyState>No payment windows yet.</EmptyState>
          )}
        </Card>
      </div>

      <div className="mt-4 grid gap-3 lg:grid-cols-3">
        <Card title="Business snapshot (last hour)" className="lg:col-span-2">
          {summary.data ? (
            <div className="grid grid-cols-2 gap-x-6 gap-y-1.5 text-sm md:grid-cols-3">
              <Metric label="Orders created" v={num(summary.data.metrics.orders_created?.sum)} />
              <Metric label="Cancellations" v={num(summary.data.metrics.orders_cancelled?.sum)} />
              <Metric
                label="Cancel rate"
                v={pct(summary.data.metrics.order_cancellation_rate?.latest)}
              />
              <Metric label="Revenue" v={money(summary.data.metrics.revenue?.sum)} />
              <Metric
                label="Avg order value"
                v={money(summary.data.metrics.avg_order_value?.latest)}
              />
              <Metric
                label="Payment success"
                v={pct(summary.data.metrics.payment_success_rate?.latest)}
              />
              <Metric label="Active users" v={num(summary.data.metrics.active_users?.max)} />
              <Metric label="Logins" v={num(summary.data.metrics.logins?.sum)} />
              <Metric
                label="Conversion"
                v={pct(summary.data.metrics.conversion_rate?.latest)}
              />
            </div>
          ) : (
            <Spinner />
          )}
        </Card>

        <Card title="Recent anomalies">
          {anomalies.data && anomalies.data.items.length > 0 ? (
            <ul className="space-y-2 text-sm">
              {anomalies.data.items.map((a) => (
                <li key={a.id}>
                  <Link
                    href={`/anomalies/${a.id}`}
                    className="flex items-center justify-between gap-2 hover:text-accent"
                  >
                    <span className="truncate">{a.metric}</span>
                    <span className="flex shrink-0 items-center gap-2">
                      <Badge tone={severityTone(a.severity)}>{a.severity}</Badge>
                      <span className="text-xs text-muted">{ago(a.detected_at)}</span>
                    </span>
                  </Link>
                </li>
              ))}
            </ul>
          ) : (
            <EmptyState>No anomalies in the last 24h.</EmptyState>
          )}
        </Card>
      </div>
    </div>
  );
}

function Metric({ label, v }: { label: string; v: string }) {
  return (
    <div className="flex items-center justify-between border-b border-border/50 py-1">
      <span className="text-muted">{label}</span>
      <span className="tabular font-medium">{v}</span>
    </div>
  );
}
