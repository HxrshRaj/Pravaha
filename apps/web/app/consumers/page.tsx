"use client";

import { PageHeader } from "@/components/shell";
import { Badge, Card, EmptyState, Row, Spinner, Table, Td, Th } from "@/components/ui";
import { ago, num } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import Link from "next/link";

export default function ConsumersPage() {
  const { data, loading } = useApi<any[]>("/api/v1/consumers/groups", { intervalMs: 6000 });

  return (
    <div>
      <PageHeader title="Consumer Groups" desc="Independent consumer groups, one per processing concern. Each has its own offsets." />
      {loading && !data ? (
        <Spinner />
      ) : !data || data.length === 0 ? (
        <EmptyState>No consumer groups registered yet (lag-monitor registers them on start).</EmptyState>
      ) : (
        <div className="space-y-3">
          {data.map((g) => (
            <Card
              key={g.name}
              title={
                <span className="flex items-center gap-2">
                  {g.name}
                  <Badge tone={g.total_lag > 2000 ? "danger" : g.total_lag > 200 ? "warn" : "ok"}>
                    lag {num(g.total_lag)}
                  </Badge>
                  <span className="text-xs font-normal text-muted">{g.active_instances} instance(s)</span>
                </span>
              }
              right={<Link href={`/consumers/lag?group=${g.name}`} className="text-xs text-accent hover:underline">lag history →</Link>}
            >
              <div className="grid gap-3 lg:grid-cols-2">
                <div>
                  <div className="mb-1 text-xs uppercase tracking-wide text-muted">Partitions</div>
                  <Table head={<><Th>Topic</Th><Th>P</Th><Th>Committed</Th><Th>LEO</Th><Th>Lag</Th></>}>
                    {g.partitions.map((p: any) => (
                      <Row key={p.topic + p.partition}>
                        <Td className="font-mono text-xs">{p.topic}</Td>
                        <Td>{p.partition}</Td>
                        <Td>{num(p.current_offset)}</Td>
                        <Td>{num(p.log_end_offset)}</Td>
                        <Td className={p.lag > 500 ? "text-danger" : ""}>{num(p.lag)}</Td>
                      </Row>
                    ))}
                  </Table>
                </div>
                <div>
                  <div className="mb-1 text-xs uppercase tracking-wide text-muted">Instances</div>
                  {g.instances.length === 0 ? (
                    <EmptyState>No instances reporting in the last 2m.</EmptyState>
                  ) : (
                    <Table head={<><Th>Member</Th><Th>State</Th><Th>Processed</Th><Th>Failed</Th><Th>p95 ms</Th><Th>Seen</Th></>}>
                      {g.instances.map((i: any) => (
                        <Row key={i.member_id}>
                          <Td className="font-mono text-xs">{i.member_id}</Td>
                          <Td>{i.paused ? <Badge tone="warn">paused</Badge> : <Badge tone="ok">{i.state}</Badge>}</Td>
                          <Td>{num(i.events_processed)}</Td>
                          <Td>{num(i.events_failed)}</Td>
                          <Td>{num(i.p95_latency_ms, 1)}</Td>
                          <Td className="text-muted">{ago(i.last_seen)}</Td>
                        </Row>
                      ))}
                    </Table>
                  )}
                </div>
              </div>
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}
