"use client";

import { PageHeader } from "@/components/shell";
import { Badge, Card, EmptyState, ErrorState, Row, Spinner, Table, Td, Th } from "@/components/ui";
import { ago, shortId } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { Suspense, useState } from "react";

function EventsInner() {
  const sp = useSearchParams();
  const [eventType, setEventType] = useState(sp.get("event_type") ?? "");
  const [corr, setCorr] = useState(sp.get("correlation_id") ?? "");
  const [offset, setOffset] = useState(0);
  const limit = 50;

  const qs = new URLSearchParams({ limit: String(limit), offset: String(offset) });
  if (eventType) qs.set("event_type", eventType);
  if (corr) qs.set("correlation_id", corr);

  const { data, error, loading } = useApi<{
    items: any[];
    meta: { total: number };
  }>(`/api/v1/events?${qs.toString()}`, { intervalMs: 8000, deps: [offset, eventType, corr] });

  return (
    <div>
      <PageHeader title="Events" desc="Searchable event store (PostgreSQL). Indexed by type, producer, event_time, correlation." />
      <div className="mb-3 flex flex-wrap gap-2">
        <input
          placeholder="event_type"
          className="w-48 rounded border border-border bg-bg px-2 py-1 text-sm outline-none focus:border-accent"
          value={eventType}
          onChange={(e) => { setEventType(e.target.value.trim()); setOffset(0); }}
        />
        <input
          placeholder="correlation_id"
          className="w-64 rounded border border-border bg-bg px-2 py-1 text-sm outline-none focus:border-accent"
          value={corr}
          onChange={(e) => { setCorr(e.target.value.trim()); setOffset(0); }}
        />
      </div>

      {error ? (
        <ErrorState message={error.message} />
      ) : loading && !data ? (
        <Spinner />
      ) : !data || data.items.length === 0 ? (
        <EmptyState>No events match.</EmptyState>
      ) : (
        <Card
          title={`${data.meta.total.toLocaleString()} events`}
          right={
            <div className="flex gap-2 text-xs">
              <button disabled={offset === 0} className="rounded border border-border px-2 py-0.5 disabled:opacity-40" onClick={() => setOffset(Math.max(0, offset - limit))}>Prev</button>
              <button disabled={offset + limit >= data.meta.total} className="rounded border border-border px-2 py-0.5 disabled:opacity-40" onClick={() => setOffset(offset + limit)}>Next</button>
            </div>
          }
        >
          <Table
            head={<><Th>Time</Th><Th>Type</Th><Th>Producer</Th><Th>Region</Th><Th>Part/Off</Th><Th>Correlation</Th><Th>Late</Th></>}
          >
            {data.items.map((e) => (
              <Row key={e.event_id}>
                <Td className="whitespace-nowrap text-muted">{new Date(e.event_time).toLocaleTimeString()}</Td>
                <Td><Link href={`/events/${e.event_id}`} className="text-accent hover:underline">{e.event_type}</Link></Td>
                <Td>{e.producer}</Td>
                <Td className="text-muted">{e.region ?? "—"}</Td>
                <Td>{e.kafka_partition ?? "—"}/{e.kafka_offset ?? "—"}</Td>
                <Td className="font-mono text-xs">{shortId(e.correlation_id, 16)}</Td>
                <Td>{e.lateness_at_ingest !== "on_time" ? <Badge tone="warn">{e.lateness_at_ingest}</Badge> : ""}</Td>
              </Row>
            ))}
          </Table>
        </Card>
      )}
    </div>
  );
}

export default function EventsPage() {
  return (
    <Suspense fallback={<Spinner />}>
      <EventsInner />
    </Suspense>
  );
}
