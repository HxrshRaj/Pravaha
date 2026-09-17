"use client";

import { PageHeader } from "@/components/shell";
import { Card, EmptyState, Row, Spinner, StatCard, Table, Td, Th } from "@/components/ui";
import { ago, money, num } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import { useState } from "react";

type Rollup = {
  id: string;
  event_type: string;
  region: string;
  hour_bucket: string;
  event_count: number;
  distinct_correlation_ids: number;
  distinct_users: number;
  total_amount: number;
  avg_amount: number;
  late_event_count: number;
  replay_event_count: number;
  computed_at: string;
};

type Summary = {
  window_hours: number;
  rollup_rows: number;
  total_events: number;
  total_amount: number;
  late_events: number;
  replay_events: number;
  last_computed_at: string | null;
  earliest_hour_bucket: string | null;
  latest_hour_bucket: string | null;
  top_event_types: { event_type: string; event_count: number }[];
};

const WINDOW_HOURS = 24 * 30; // batch rollups are hourly, not live - show a wide default window

export default function BatchPage() {
  const [eventType, setEventType] = useState<string | null>(null);

  const { data: summary, loading: loadingSummary } = useApi<Summary>(
    `/api/v1/batch/rollups/summary?hours=${WINDOW_HOURS}`,
    { intervalMs: 30000 },
  );
  const { data: rollups, loading: loadingRollups } = useApi<{ items: Rollup[]; meta: { total: number } }>(
    `/api/v1/batch/rollups?hours=${WINDOW_HOURS}&limit=200${eventType ? `&event_type=${encodeURIComponent(eventType)}` : ""}`,
    { intervalMs: 30000, deps: [eventType] },
  );

  const maxTop = Math.max(1, ...(summary?.top_event_types ?? []).map((t) => t.event_count));

  return (
    <div>
      <PageHeader
        title="Batch Analytics"
        desc="Hourly rollups computed offline by the Scala/Spark batch job, re-derived from the durable events table (not the live speed layer). See README §24."
      />

      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <StatCard label="Rollup rows" value={num(summary?.rollup_rows ?? 0)} />
        <StatCard label="Events aggregated" value={num(summary?.total_events ?? 0)} />
        <StatCard label="Revenue rolled up" value={money(summary?.total_amount ?? 0)} />
        <StatCard
          label="Last batch run"
          value={summary?.last_computed_at ? ago(summary.last_computed_at) : "—"}
        />
      </div>

      <Card title="Top event types (by count)" className="mt-3">
        {loadingSummary && !summary ? (
          <Spinner />
        ) : !summary || summary.top_event_types.length === 0 ? (
          <EmptyState>No batch rollups yet. Run the Spark job: see spark/README.md.</EmptyState>
        ) : (
          <div className="space-y-2">
            {summary.top_event_types.map((t) => (
              <button
                key={t.event_type}
                onClick={() => setEventType(eventType === t.event_type ? null : t.event_type)}
                className="flex w-full items-center gap-3 text-left"
                title="filter the table below to this event type"
              >
                <span className={`w-40 truncate text-xs ${eventType === t.event_type ? "font-semibold text-fg" : "text-muted"}`}>
                  {t.event_type}
                </span>
                <span className="h-4 flex-1 overflow-hidden rounded bg-border">
                  <span
                    className="block h-full rounded bg-accent"
                    style={{ width: `${(t.event_count / maxTop) * 100}%` }}
                  />
                </span>
                <span className="w-16 text-right text-xs tabular-nums text-muted">{num(t.event_count)}</span>
              </button>
            ))}
          </div>
        )}
      </Card>

      <Card
        title="Hourly rollups"
        className="mt-3"
        right={
          eventType && (
            <button className="text-xs text-accent underline" onClick={() => setEventType(null)}>
              clear filter ({eventType})
            </button>
          )
        }
      >
        {loadingRollups && !rollups ? (
          <Spinner />
        ) : !rollups || rollups.items.length === 0 ? (
          <EmptyState>
            No batch rollups yet. From the repo root: <code>alembic upgrade head</code>, then from{" "}
            <code>spark/</code>: <code>sbt run</code>.
          </EmptyState>
        ) : (
          <Table
            head={
              <>
                <Th>Hour (UTC)</Th>
                <Th>Event type</Th>
                <Th>Region</Th>
                <Th>Events</Th>
                <Th>Distinct txns</Th>
                <Th>Distinct users</Th>
                <Th>Total amount</Th>
                <Th>Avg amount</Th>
                <Th>Late</Th>
                <Th>Replay</Th>
              </>
            }
          >
            {rollups.items.map((r) => (
              <Row key={r.id}>
                <Td>{r.hour_bucket.replace("T", " ").slice(0, 16)}</Td>
                <Td>{r.event_type}</Td>
                <Td>{r.region}</Td>
                <Td>{num(r.event_count)}</Td>
                <Td>{num(r.distinct_correlation_ids)}</Td>
                <Td>{num(r.distinct_users)}</Td>
                <Td>{r.total_amount ? money(r.total_amount) : "—"}</Td>
                <Td>{r.avg_amount ? money(r.avg_amount) : "—"}</Td>
                <Td className={r.late_event_count ? "text-warn" : ""}>{num(r.late_event_count)}</Td>
                <Td className={r.replay_event_count ? "text-warn" : ""}>{num(r.replay_event_count)}</Td>
              </Row>
            ))}
          </Table>
        )}
      </Card>
    </div>
  );
}
