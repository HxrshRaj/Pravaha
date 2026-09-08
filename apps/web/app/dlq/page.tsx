"use client";

import { PageHeader } from "@/components/shell";
import { Badge, Card, EmptyState, Row, Spinner, StatCard, Table, Td, Th } from "@/components/ui";
import { api } from "@/lib/api";
import { ago } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import { useState } from "react";

export default function DLQPage() {
  const stats = useApi<any>("/api/v1/dlq/stats", { intervalMs: 8000 });
  const [status, setStatus] = useState("PENDING");
  const list = useApi<{ items: any[]; meta: { total: number } }>(
    `/api/v1/dlq?status=${status}&limit=100`,
    { intervalMs: 8000, deps: [status] },
  );
  const [detail, setDetail] = useState<any | null>(null);
  const [msg, setMsg] = useState<string | null>(null);

  async function act(id: string, action: "retry" | "discard") {
    setMsg(null);
    try {
      const path =
        action === "discard"
          ? `/api/v1/dlq/${id}/discard?confirm=true`
          : `/api/v1/dlq/${id}/retry`;
      await api(path, { method: "POST" });
      setMsg(`${action} ok for ${id.slice(0, 8)}`);
      list.refetch();
      stats.refetch();
    } catch (e: any) {
      setMsg(e?.message ?? "failed");
    }
  }

  return (
    <div>
      <PageHeader title="Dead Letter Queue" desc="Events that exhausted retries or failed validation. Retry re-publishes to events.raw; idempotency prevents duplicate effects." />
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <StatCard label="Pending" value={stats.data?.pending ?? 0} tone={stats.data?.pending ? "danger" : "ok"} />
        <StatCard label="Retried" value={stats.data?.by_status?.RETRIED ?? 0} />
        <StatCard label="Discarded" value={stats.data?.by_status?.DISCARDED ?? 0} />
        <StatCard label="Resolved" value={stats.data?.by_status?.RESOLVED ?? 0} />
      </div>
      {msg && <div className="mt-2 text-sm text-accent">{msg}</div>}

      <Card
        title="Entries"
        className="mt-3"
        right={
          <select className="rounded border border-border bg-bg px-2 py-1 text-xs" value={status} onChange={(e) => setStatus(e.target.value)}>
            {["PENDING", "RETRIED", "DISCARDED", "RESOLVED"].map((s) => <option key={s}>{s}</option>)}
          </select>
        }
      >
        {list.loading && !list.data ? <Spinner /> : !list.data || list.data.items.length === 0 ? (
          <EmptyState>Nothing in {status}.</EmptyState>
        ) : (
          <Table head={<><Th>When</Th><Th>Processor</Th><Th>Reason</Th><Th>Class</Th><Th>Attempts</Th><Th>Event</Th><Th /></>}>
            {list.data.items.map((d) => (
              <Row key={d.id} onClick={() => setDetail(d)}>
                <Td className="whitespace-nowrap text-muted">{ago(d.created_at)}</Td>
                <Td>{d.processor}</Td>
                <Td><Badge tone="danger">{d.failure_reason}</Badge></Td>
                <Td className="text-xs text-muted">{d.error_class}</Td>
                <Td>{d.attempt_count}</Td>
                <Td className="font-mono text-xs">{d.event_id?.slice(0, 8) ?? "—"}</Td>
                <Td>
                  {d.status === "PENDING" && (
                    <span className="flex gap-1" onClick={(e) => e.stopPropagation()}>
                      <button className="rounded border border-border px-1.5 py-0.5 text-xs hover:bg-surface2" onClick={() => act(d.id, "retry")}>retry</button>
                      <button className="rounded border border-danger/40 px-1.5 py-0.5 text-xs text-danger hover:bg-danger/10" onClick={() => act(d.id, "discard")}>discard</button>
                    </span>
                  )}
                </Td>
              </Row>
            ))}
          </Table>
        )}
      </Card>

      {detail && (
        <Card title={`DLQ ${detail.id.slice(0, 8)}`} className="mt-3" right={<button className="text-xs text-accent" onClick={() => setDetail(null)}>close</button>}>
          <pre className="max-h-72 overflow-auto rounded bg-bg p-2 font-mono text-xs">{detail.error_detail}</pre>
        </Card>
      )}
    </div>
  );
}
