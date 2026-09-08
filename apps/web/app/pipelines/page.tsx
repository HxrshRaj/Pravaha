"use client";

import { PageHeader } from "@/components/shell";
import { Badge, Card, EmptyState, Row, Spinner, Table, Td, Th } from "@/components/ui";
import { api } from "@/lib/api";
import { useApi } from "@/lib/hooks";
import { useState } from "react";

const SAMPLE = {
  name: "payment-failure-rate-1m",
  description: "1-minute payment failure rate",
  graph: {
    nodes: [
      { key: "src", type: "source", config: { topic: "events.validated" } },
      { key: "flt", type: "filter", config: { clauses: [{ field: "event_type", op: "in", value: ["payment.completed", "payment.failed"] }] } },
      { key: "win", type: "window", config: { window_type: "tumbling", size: "1m" } },
      { key: "agg", type: "aggregate", config: { kind: "rate", field: "event_type", op: "eq", value: "payment.failed" } },
      { key: "snk", type: "sink", config: { metric: "pipeline_payment_failure_rate" } },
    ],
    edges: [
      { from: "src", to: "flt" },
      { from: "flt", to: "win" },
      { from: "win", to: "agg" },
      { from: "agg", to: "snk" },
    ],
  },
};

export default function PipelinesPage() {
  const { data, loading, refetch } = useApi<any[]>("/api/v1/pipelines", { intervalMs: 15000 });
  const [text, setText] = useState(JSON.stringify(SAMPLE, null, 2));
  const [validation, setValidation] = useState<any | null>(null);
  const [msg, setMsg] = useState<string | null>(null);

  async function validate() {
    setMsg(null);
    try {
      const parsed = JSON.parse(text);
      const r = await api("/api/v1/pipelines/validate", { method: "POST", body: JSON.stringify(parsed) });
      setValidation(r);
    } catch (e: any) {
      setMsg(e?.message ?? "invalid JSON");
    }
  }
  async function create() {
    try {
      const parsed = JSON.parse(text);
      await api("/api/v1/pipelines", { method: "POST", body: JSON.stringify(parsed) });
      setMsg("pipeline draft created");
      refetch();
    } catch (e: any) {
      setMsg(e?.message ?? "create failed");
    }
  }
  async function publish(id: string, version: number) {
    try {
      await api(`/api/v1/pipelines/${id}/versions/${version}/publish`, { method: "POST" });
      setMsg("published");
      refetch();
    } catch (e: any) {
      setMsg(e?.message ?? "publish failed");
    }
  }

  return (
    <div>
      <PageHeader title="Pipelines" desc="Config-driven processing graphs: source → filter → transform → group → window → aggregate → sink. Published versions are immutable." />
      <div className="grid gap-3 lg:grid-cols-2">
        <Card title="Pipeline definition (JSON)">
          <textarea
            className="h-72 w-full rounded border border-border bg-bg p-2 font-mono text-xs outline-none focus:border-accent"
            value={text}
            onChange={(e) => setText(e.target.value)}
          />
          <div className="mt-2 flex gap-2">
            <button className="rounded border border-border px-3 py-1.5 text-sm hover:bg-surface2" onClick={validate}>Validate</button>
            <button className="rounded bg-accent px-3 py-1.5 text-sm font-medium text-white" onClick={create}>Create draft</button>
            {msg && <span className="self-center text-xs text-accent">{msg}</span>}
          </div>
        </Card>
        <Card title="Validation">
          {!validation ? (
            <EmptyState>Run validation.</EmptyState>
          ) : (
            <div className="text-sm">
              <Badge tone={validation.ok ? "ok" : "danger"}>{validation.ok ? "valid" : "invalid"}</Badge>
              {validation.errors?.length > 0 && (
                <ul className="mt-2 list-disc space-y-0.5 pl-5 text-danger">
                  {validation.errors.map((e: string, i: number) => <li key={i}>{e}</li>)}
                </ul>
              )}
              {validation.warnings?.length > 0 && (
                <ul className="mt-2 list-disc space-y-0.5 pl-5 text-warn">
                  {validation.warnings.map((e: string, i: number) => <li key={i}>{e}</li>)}
                </ul>
              )}
              {validation.topological_order?.length > 0 && (
                <div className="mt-2 font-mono text-xs text-muted">
                  order: {validation.topological_order.join(" → ")}
                </div>
              )}
            </div>
          )}
        </Card>
      </div>

      <Card title="Pipelines" className="mt-3">
        {loading && !data ? <Spinner /> : !data || data.length === 0 ? (
          <EmptyState>No pipelines yet.</EmptyState>
        ) : (
          <Table head={<><Th>Name</Th><Th>Versions</Th><Th>Active</Th><Th /></>}>
            {data.map((p) => (
              <Row key={p.id}>
                <Td>{p.name}</Td>
                <Td className="text-xs">
                  {p.versions.map((v: any) => (
                    <span key={v.id} className="mr-1">
                      <Badge tone={v.status === "PUBLISHED" ? "ok" : v.status === "DISABLED" ? "default" : "accent"}>
                        v{v.version}:{v.status}
                      </Badge>
                    </span>
                  ))}
                </Td>
                <Td>{p.active_version_id ? "yes" : "—"}</Td>
                <Td>
                  {p.versions.filter((v: any) => v.status === "DRAFT").map((v: any) => (
                    <button key={v.id} className="rounded border border-border px-2 py-0.5 text-xs hover:bg-surface2" onClick={() => publish(p.id, v.version)}>
                      publish v{v.version}
                    </button>
                  ))}
                </Td>
              </Row>
            ))}
          </Table>
        )}
      </Card>
    </div>
  );
}
