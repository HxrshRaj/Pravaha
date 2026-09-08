"use client";

import { PageHeader } from "@/components/shell";
import { Badge, Card, EmptyState, ErrorState, Row, Spinner, StatCard, Table, Td, Th } from "@/components/ui";
import { num } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import Link from "next/link";
import { useParams } from "next/navigation";

export default function InvestigationPage() {
  const { id } = useParams<{ id: string }>();
  const { data: inv, error, loading } = useApi<any>(`/api/v1/ai/investigations/${id}`, {
    intervalMs: 4000,
  });

  if (error) return <ErrorState message={error.message} />;
  if (loading || !inv) return <Spinner />;

  const running = ["RUNNING", "CREATED", "WAITING_FOR_TOOL"].includes(inv.status);

  return (
    <div>
      <PageHeader
        title={inv.title || "Investigation"}
        desc={
          <>
            anomaly <Link href={`/anomalies/${inv.anomaly_id}`} className="text-accent hover:underline">{inv.anomaly_id?.slice(0, 8)}</Link>
            {" · "}prompt {inv.prompt_version} · {inv.provider ?? "—"}/{inv.model ?? "—"}
          </>
        }
        right={
          <Badge tone={inv.status === "COMPLETED" ? "ok" : inv.status === "FAILED" ? "danger" : "accent"}>
            {inv.status}{running ? ` · ${inv.phase}` : ""}
          </Badge>
        }
      />

      {inv.status === "FAILED" && (
        <div className="mb-3 rounded border border-danger/40 bg-danger/10 p-2 text-sm text-danger">
          {inv.error} — core event processing was unaffected.
        </div>
      )}

      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <StatCard label="Confidence" value={num(inv.confidence, 2)} />
        <StatCard label="Tool calls" value={num(inv.tool_calls?.length ?? 0)} />
        <StatCard label="Evidence refs" value={num(inv.evidence?.length ?? 0)} />
        <StatCard label="Est. cost" value={`$${num(inv.usage?.estimated_cost_usd ?? 0, 5)}`} sub={`${num(inv.usage?.total_tokens ?? 0)} tok`} />
      </div>

      {inv.status === "COMPLETED" && (
        <Card title="Conclusion" className="mt-3">
          <p className="text-sm">{inv.summary}</p>
          <div className="mt-2 grid gap-2 text-sm md:grid-cols-2">
            <div><span className="text-muted">Root cause: </span><strong>{inv.root_cause}</strong></div>
            <div><span className="text-muted">Impact: </span>{inv.impact}</div>
          </div>
          {inv.recommendations?.length > 0 && (
            <>
              <div className="mt-3 text-xs uppercase tracking-wide text-muted">Recommendations</div>
              <ul className="mt-1 list-disc space-y-1 pl-5 text-sm">
                {inv.recommendations.map((r: string, i: number) => <li key={i}>{r}</li>)}
              </ul>
            </>
          )}
        </Card>
      )}

      <Card title="Hypotheses" className="mt-3">
        {inv.hypotheses?.length ? (
          <div className="space-y-2">
            {inv.hypotheses.map((h: any) => (
              <div key={h.key} className="rounded border border-border p-2 text-sm">
                <div className="flex items-center justify-between">
                  <span className="font-medium">{h.key}. {h.statement}</span>
                  <span className="flex items-center gap-2">
                    <Badge tone={h.status === "SELECTED" ? "ok" : "default"}>{h.status}</Badge>
                    <span className="tabular text-xs">{num(h.confidence, 2)}</span>
                  </span>
                </div>
                {h.supporting?.length > 0 && (
                  <div className="mt-1 text-xs text-ok">+ {h.supporting.join("; ")}</div>
                )}
                {h.contradicting?.length > 0 && (
                  <div className="text-xs text-danger">− {h.contradicting.join("; ")}</div>
                )}
                {h.evidence_refs?.length > 0 && (
                  <div className="mt-1 font-mono text-[11px] text-muted">{h.evidence_refs.join("  ")}</div>
                )}
              </div>
            ))}
          </div>
        ) : running ? (
          <Spinner label="Reasoning…" />
        ) : (
          <EmptyState>No hypotheses.</EmptyState>
        )}
      </Card>

      <div className="mt-3 grid gap-3 lg:grid-cols-2">
        <Card title="Evidence collected">
          {inv.evidence?.length ? (
            <ul className="space-y-1 font-mono text-xs">
              {inv.evidence.map((e: any, i: number) => (
                <li key={i} className="border-b border-border/40 py-0.5">
                  <span className="text-accent">{e.ref}</span>{" "}
                  <span className="text-muted">via {e.source_tool}</span>
                </li>
              ))}
            </ul>
          ) : (
            <EmptyState>None yet.</EmptyState>
          )}
        </Card>
        <Card title="Tool calls (read-only, allow-listed)">
          <Table head={<><Th>#</Th><Th>Tool</Th><Th>OK</Th><Th>ms</Th><Th>Result</Th></>}>
            {(inv.tool_calls ?? []).map((t: any) => (
              <Row key={t.seq}>
                <Td>{t.seq}</Td>
                <Td className="font-mono text-xs">{t.tool_name}</Td>
                <Td>{t.ok ? <Badge tone="ok">ok</Badge> : <Badge tone="danger">err</Badge>}</Td>
                <Td>{t.latency_ms}</Td>
                <Td className="text-xs text-muted">{t.error ?? t.result_summary}</Td>
              </Row>
            ))}
          </Table>
        </Card>
      </div>
    </div>
  );
}
