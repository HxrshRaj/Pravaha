"use client";

import { PageHeader } from "@/components/shell";
import { Card, EmptyState, Row, Spinner, Table, Td, Th } from "@/components/ui";
import { ago } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import { useState } from "react";

export default function AuditPage() {
  const [offset, setOffset] = useState(0);
  const limit = 60;
  const { data, loading, error } = useApi<{ items: any[]; meta: { total: number } }>(
    `/api/v1/audit?limit=${limit}&offset=${offset}&since_hours=336`,
    { intervalMs: 15000, deps: [offset] },
  );

  return (
    <div>
      <PageHeader title="Audit Log" desc="Append-only record of sensitive actions: producer/schema/pipeline changes, replay, DLQ actions, auth events." />
      {error ? (
        <div className="text-sm text-danger">{error.message} (ENGINEER role required)</div>
      ) : loading && !data ? (
        <Spinner />
      ) : !data || data.items.length === 0 ? (
        <EmptyState>No audit entries.</EmptyState>
      ) : (
        <Card
          title={`${data.meta.total} entries`}
          right={
            <div className="flex gap-2 text-xs">
              <button disabled={offset === 0} className="rounded border border-border px-2 py-0.5 disabled:opacity-40" onClick={() => setOffset(Math.max(0, offset - limit))}>Prev</button>
              <button disabled={offset + limit >= data.meta.total} className="rounded border border-border px-2 py-0.5 disabled:opacity-40" onClick={() => setOffset(offset + limit)}>Next</button>
            </div>
          }
        >
          <Table head={<><Th>When</Th><Th>Actor</Th><Th>Role</Th><Th>Action</Th><Th>Resource</Th><Th>Result</Th></>}>
            {data.items.map((a) => (
              <Row key={a.id}>
                <Td className="whitespace-nowrap text-muted">{ago(a.created_at)}</Td>
                <Td>{a.actor}</Td>
                <Td className="text-xs text-muted">{a.actor_role}</Td>
                <Td className="font-mono text-xs">{a.action}</Td>
                <Td className="text-xs">{a.resource_type}{a.resource_id ? `/${a.resource_id.slice(0, 8)}` : ""}</Td>
                <Td className={a.result === "success" ? "text-ok" : "text-danger"}>{a.result}</Td>
              </Row>
            ))}
          </Table>
        </Card>
      )}
    </div>
  );
}
