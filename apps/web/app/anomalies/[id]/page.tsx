"use client";

import { TimeSeries } from "@/components/charts";
import { PageHeader } from "@/components/shell";
import { Badge, Card, EmptyState, ErrorState, Spinner, StatCard, severityTone } from "@/components/ui";
import { api } from "@/lib/api";
import { ago, num } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import Link from "next/link";
import { useParams } from "next/navigation";
import { useState } from "react";

export default function AnomalyDetailPage() {
  const { id } = useParams<{ id: string }>();
  const { data: a, error, loading, refetch } = useApi<any>(`/api/v1/anomalies/${id}`, { intervalMs: 8000 });
  const { data: ev } = useApi<any>(`/api/v1/anomalies/${id}/evidence`);
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);

  async function investigate() {
    setBusy(true);
    setMsg(null);
    try {
      const r = await api<{ investigation_id: string }>("/api/v1/ai/investigations", {
        method: "POST",
        body: JSON.stringify({ anomaly_id: id }),
      });
      setMsg(`Investigation ${r.investigation_id.slice(0, 8)} started`);
      setTimeout(refetch, 1500);
    } catch (e: any) {
      setMsg(e?.message ?? "failed to start");
    } finally {
      setBusy(false);
    }
  }

  if (error) return <ErrorState message={error.message} />;
  if (loading || !a) return <Spinner />;

  return (
    <div>
      <PageHeader
        title={`Anomaly · ${a.metric}`}
        desc={`${a.algorithm} · baseline ${a.baseline_kind} · detected ${ago(a.detected_at)}`}
        right={
          <button
            onClick={investigate}
            disabled={busy}
            className="rounded bg-accent px-3 py-1.5 text-sm font-medium text-white disabled:opacity-60"
          >
            {busy ? "Starting…" : "Run AI investigation"}
          </button>
        }
      />
      {msg && <div className="mb-3 text-sm text-accent">{msg}</div>}

      <div className="grid grid-cols-2 gap-3 md:grid-cols-5">
        <StatCard label="Severity" value={<Badge tone={severityTone(a.severity)}>{a.severity}</Badge>} />
        <StatCard label="Observed" value={num(a.observed_value, 3)} />
        <StatCard label="Expected" value={num(a.expected_value, 3)} />
        <StatCard label="Deviation" value={num(a.deviation, 3)} />
        <StatCard label="Confidence" value={num(a.confidence, 2)} />
      </div>

      <div className="mt-3 grid gap-3 lg:grid-cols-2">
        <Card title="Metric around the window">
          {a.series?.length ? (
            <TimeSeries data={a.series} series={[{ key: "value", label: a.metric }]} area />
          ) : (
            <EmptyState>No series.</EmptyState>
          )}
        </Card>
        <Card title="Detector evidence">
          <pre className="max-h-64 overflow-auto rounded bg-bg p-2 font-mono text-xs">
            {JSON.stringify(a.evidence, null, 2)}
          </pre>
        </Card>
      </div>

      <Card title="Event evidence (window ±)" className="mt-3">
        {!ev ? (
          <Spinner />
        ) : (
          <>
            <div className="mb-2 flex flex-wrap gap-2 text-xs">
              {Object.entries(ev.event_type_counts ?? {}).map(([t, c]) => (
                <Badge key={t}>{t}: {c as number}</Badge>
              ))}
            </div>
            <div className="max-h-60 overflow-auto font-mono text-xs">
              {(ev.sample_events ?? []).map((s: any) => (
                <div key={s.event_id} className="border-b border-border/40 py-0.5">
                  <span className="text-muted">{new Date(s.event_time).toLocaleTimeString()} </span>
                  <span className="text-accent">{s.event_type}</span>
                  {s.reason && <span className="text-danger"> reason={s.reason}</span>}
                  {s.amount != null && <span className="text-muted"> ${s.amount}</span>}{" "}
                  <Link href={`/events/${s.event_id}`} className="text-accent hover:underline">↗</Link>
                </div>
              ))}
            </div>
          </>
        )}
      </Card>

      <Card title="AI investigations" className="mt-3">
        {a.investigations?.length ? (
          <ul className="space-y-1 text-sm">
            {a.investigations.map((i: any) => (
              <li key={i.id} className="flex items-center justify-between border-b border-border/50 py-1">
                <Link href={`/ai/${i.id}`} className="text-accent hover:underline">
                  {i.id.slice(0, 8)} · {i.phase}
                </Link>
                <span className="flex items-center gap-2">
                  <Badge tone={i.status === "COMPLETED" ? "ok" : i.status === "FAILED" ? "danger" : "accent"}>
                    {i.status}
                  </Badge>
                  {i.root_cause && <span className="text-xs text-muted">{i.root_cause}</span>}
                </span>
              </li>
            ))}
          </ul>
        ) : (
          <EmptyState>No investigations yet.</EmptyState>
        )}
      </Card>
    </div>
  );
}
