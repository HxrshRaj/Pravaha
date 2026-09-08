"use client";

import { PageHeader } from "@/components/shell";
import { Badge, Card, EmptyState, Row, Spinner, StatCard, Table, Td, Th } from "@/components/ui";
import { ago, num } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import Link from "next/link";

export default function AIPage() {
  const { data: status } = useApi<any>("/api/v1/ai/status", { intervalMs: 20000 });
  const { data: usage } = useApi<any>("/api/v1/ai/usage?days=7", { intervalMs: 30000 });
  const { data: invs, loading } = useApi<any[]>("/api/v1/ai/investigations?limit=50", {
    intervalMs: 8000,
  });

  return (
    <div>
      <PageHeader
        title="AI Intelligence"
        desc="Evidence-grounded anomaly investigations. Core streaming never depends on AI availability."
        right={
          <Badge tone={status?.available_providers?.length ? "ok" : "warn"}>
            providers: {(status?.available_providers ?? []).join(", ") || "none"}
          </Badge>
        }
      />

      {status && !status.available_providers?.length && (
        <div className="mb-3 rounded border border-warn/40 bg-warn/10 p-2 text-sm text-warn">
          AI unavailable — core event processing continues normally.
        </div>
      )}

      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <StatCard label="Investigations (7d)" value={num(usage?.by_model?.reduce((a: number, m: any) => a + m.calls, 0) ?? 0)} sub="provider calls" />
        <StatCard label="Tokens (7d)" value={num(usage?.total_tokens ?? 0)} />
        <StatCard label="Est. cost (7d)" value={`$${num(usage?.total_cost_usd ?? 0, 4)}`} />
        <StatCard label="Prompt version" value="v1" />
      </div>

      <Card title="Investigations" className="mt-3">
        {loading && !invs ? (
          <Spinner />
        ) : !invs || invs.length === 0 ? (
          <EmptyState>No investigations yet. Open an anomaly and click “Run AI investigation”.</EmptyState>
        ) : (
          <Table head={<><Th>Created</Th><Th>Title</Th><Th>Status</Th><Th>Provider</Th><Th>Root cause</Th><Th>Conf.</Th><Th>Duration</Th></>}>
            {invs.map((i) => (
              <Row key={i.id}>
                <Td className="whitespace-nowrap text-muted">{ago(i.created_at)}</Td>
                <Td><Link href={`/ai/${i.id}`} className="text-accent hover:underline">{i.title || i.id.slice(0, 8)}</Link></Td>
                <Td>
                  <Badge tone={i.status === "COMPLETED" ? "ok" : i.status === "FAILED" ? "danger" : "accent"}>
                    {i.status}
                  </Badge>
                </Td>
                <Td className="text-xs text-muted">{i.provider ?? "—"}/{i.model ?? "—"}</Td>
                <Td>{i.root_cause || "—"}</Td>
                <Td>{num(i.confidence, 2)}</Td>
                <Td className="text-muted">{i.duration_ms ? `${(i.duration_ms / 1000).toFixed(1)}s` : "—"}</Td>
              </Row>
            ))}
          </Table>
        )}
      </Card>

      {usage?.by_model?.length ? (
        <Card title="Cost by model (7d)" className="mt-3">
          <Table head={<><Th>Provider</Th><Th>Model</Th><Th>Calls</Th><Th>Prompt tok</Th><Th>Compl. tok</Th><Th>Cost</Th><Th>Avg latency</Th></>}>
            {usage.by_model.map((m: any) => (
              <Row key={m.provider + m.model}>
                <Td>{m.provider}</Td>
                <Td>{m.model}</Td>
                <Td>{num(m.calls)}</Td>
                <Td>{num(m.prompt_tokens)}</Td>
                <Td>{num(m.completion_tokens)}</Td>
                <Td>${num(m.estimated_cost_usd, 5)}</Td>
                <Td className="text-muted">{num(m.avg_latency_ms)} ms</Td>
              </Row>
            ))}
          </Table>
        </Card>
      ) : null}
    </div>
  );
}
