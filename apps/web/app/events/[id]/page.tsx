"use client";

import { PageHeader } from "@/components/shell";
import { Badge, Card, EmptyState, ErrorState, Row, Spinner, Table, Td, Th } from "@/components/ui";
import { useApi } from "@/lib/hooks";
import Link from "next/link";
import { useParams } from "next/navigation";

export default function EventDetailPage() {
  const { id } = useParams<{ id: string }>();
  const { data: e, error, loading } = useApi<any>(`/api/v1/events/${id}`);
  const { data: chain } = useApi<any[]>(`/api/v1/events/${id}/correlated`);

  if (error) return <ErrorState message={error.message} />;
  if (loading || !e) return <Spinner />;

  return (
    <div>
      <PageHeader title="Event detail" desc={e.event_id} />
      <div className="grid gap-3 lg:grid-cols-2">
        <Card title="Envelope">
          <dl className="grid grid-cols-[140px_1fr] gap-y-1.5 text-sm">
            {[
              ["event_id", e.event_id],
              ["event_type", e.event_type],
              ["version", e.event_version],
              ["producer", `${e.producer}`],
              ["event_time", new Date(e.event_time).toLocaleString()],
              ["ingestion_time", new Date(e.ingestion_time).toLocaleString()],
              ["partition_key", e.partition_key],
              ["kafka topic", e.kafka_topic ?? "—"],
              ["partition / offset", `${e.kafka_partition ?? "—"} / ${e.kafka_offset ?? "—"}`],
              ["region", e.region ?? "—"],
              ["trace_id", e.trace_id],
              ["is_replay", String(e.is_replay)],
              ["amount", e.amount ?? "—"],
            ].map(([k, v]) => (
              <>
                <dt className="text-muted">{k}</dt>
                <dd className="break-all font-mono text-xs">{String(v)}</dd>
              </>
            ))}
            <dt className="text-muted">lateness</dt>
            <dd>
              <Badge tone={e.lateness_at_ingest === "on_time" ? "default" : "warn"}>
                {e.lateness_at_ingest}
              </Badge>
            </dd>
            <dt className="text-muted">correlation_id</dt>
            <dd>
              <Link href={`/events?correlation_id=${e.correlation_id}`} className="font-mono text-xs text-accent hover:underline">
                {e.correlation_id}
              </Link>
            </dd>
          </dl>
        </Card>

        <Card title="Payload & metadata">
          <div className="text-xs">
            <div className="mb-1 text-muted">payload</div>
            <pre className="max-h-64 overflow-auto rounded bg-bg p-2 font-mono">{JSON.stringify(e.payload, null, 2)}</pre>
            <div className="mb-1 mt-3 text-muted">metadata</div>
            <pre className="max-h-40 overflow-auto rounded bg-bg p-2 font-mono">{JSON.stringify(e.metadata, null, 2)}</pre>
          </div>
        </Card>
      </div>

      <Card title="Correlated event chain" className="mt-3">
        {!chain || chain.length === 0 ? (
          <EmptyState>No correlated events.</EmptyState>
        ) : (
          <Table head={<><Th>Time</Th><Th>Type</Th><Th>Region</Th><Th>Reason</Th><Th>Amount</Th><Th /></>}>
            {chain.map((c) => (
              <Row key={c.event_id}>
                <Td className="whitespace-nowrap text-muted">{new Date(c.event_time).toLocaleTimeString()}</Td>
                <Td>{c.event_id === e.event_id ? <strong>{c.event_type}</strong> : c.event_type}</Td>
                <Td className="text-muted">{c.region ?? "—"}</Td>
                <Td>{c.payload?.reason ?? "—"}</Td>
                <Td>{c.amount ?? "—"}</Td>
                <Td><Link href={`/events/${c.event_id}`} className="text-xs text-accent hover:underline">open</Link></Td>
              </Row>
            ))}
          </Table>
        )}
      </Card>
    </div>
  );
}
