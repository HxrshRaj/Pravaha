"use client";

import { PageHeader } from "@/components/shell";
import { Badge, Card, EmptyState, Row, Spinner, Table, Td, Th } from "@/components/ui";
import { api } from "@/lib/api";
import { ago, num } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import { useState } from "react";

export default function ReplayPage() {
  const jobs = useApi<any[]>("/api/v1/replay", { intervalMs: 4000 });
  const now = new Date();
  const [from, setFrom] = useState(new Date(now.getTime() - 3600_000).toISOString().slice(0, 16));
  const [to, setTo] = useState(now.toISOString().slice(0, 16));
  const [eventType, setEventType] = useState("");
  const [estimate, setEstimate] = useState<number | null>(null);
  const [msg, setMsg] = useState<string | null>(null);
  const [confirming, setConfirming] = useState(false);

  const body = () => ({
    time_from: new Date(from).toISOString(),
    time_to: new Date(to).toISOString(),
    filter_event_type: eventType || null,
    target_topic: "events.replay",
  });

  async function doEstimate() {
    setMsg(null);
    try {
      const r = await api<{ estimated_count: number }>("/api/v1/replay/estimate", {
        method: "POST",
        body: JSON.stringify(body()),
      });
      setEstimate(r.estimated_count);
      setConfirming(true);
    } catch (e: any) {
      setMsg(e?.message ?? "estimate failed");
    }
  }

  async function create() {
    try {
      await api("/api/v1/replay", { method: "POST", body: JSON.stringify(body()) });
      setMsg("replay job created");
      setConfirming(false);
      jobs.refetch();
    } catch (e: any) {
      setMsg(e?.message ?? "create failed");
    }
  }

  return (
    <div>
      <PageHeader title="Replay" desc="Re-publish historical events from the event store into events.replay. Replayed events carry is_replay=true; idempotency prevents duplicate business effects." />
      <Card title="New replay">
        <div className="grid gap-2 md:grid-cols-4">
          <label className="text-xs text-muted">From<input type="datetime-local" className="mt-1 w-full rounded border border-border bg-bg px-2 py-1 text-sm" value={from} onChange={(e) => setFrom(e.target.value)} /></label>
          <label className="text-xs text-muted">To<input type="datetime-local" className="mt-1 w-full rounded border border-border bg-bg px-2 py-1 text-sm" value={to} onChange={(e) => setTo(e.target.value)} /></label>
          <label className="text-xs text-muted">Event type (optional)<input className="mt-1 w-full rounded border border-border bg-bg px-2 py-1 text-sm" value={eventType} onChange={(e) => setEventType(e.target.value.trim())} placeholder="payment.failed" /></label>
          <label className="text-xs text-muted">Target topic<input className="mt-1 w-full rounded border border-border bg-bg px-2 py-1 text-sm" value="events.replay" disabled /></label>
        </div>
        <div className="mt-3 flex items-center gap-3">
          {!confirming ? (
            <button className="rounded bg-accent px-3 py-1.5 text-sm font-medium text-white" onClick={doEstimate}>Estimate</button>
          ) : (
            <>
              <span className="text-sm">≈ <strong>{num(estimate ?? 0)}</strong> events will be replayed.</span>
              <button className="rounded bg-accent px-3 py-1.5 text-sm font-medium text-white" onClick={create}>Confirm & run</button>
              <button className="rounded border border-border px-3 py-1.5 text-sm" onClick={() => setConfirming(false)}>Cancel</button>
            </>
          )}
          {msg && <span className="text-sm text-accent">{msg}</span>}
        </div>
      </Card>

      <Card title="Replay jobs" className="mt-3">
        {jobs.loading && !jobs.data ? <Spinner /> : !jobs.data || jobs.data.length === 0 ? (
          <EmptyState>No replay jobs.</EmptyState>
        ) : (
          <Table head={<><Th>Created</Th><Th>Range</Th><Th>Filter</Th><Th>Status</Th><Th>Progress</Th><Th>By</Th></>}>
            {jobs.data.map((j) => (
              <Row key={j.id}>
                <Td className="whitespace-nowrap text-muted">{ago(j.created_at)}</Td>
                <Td className="text-xs">{new Date(j.time_from).toLocaleTimeString()} → {new Date(j.time_to).toLocaleTimeString()}</Td>
                <Td className="text-xs">{j.filter_event_type ?? "all"}</Td>
                <Td>
                  <Badge tone={j.status === "COMPLETED" ? "ok" : j.status === "FAILED" ? "danger" : j.status === "RUNNING" ? "accent" : "default"}>
                    {j.status}
                  </Badge>
                </Td>
                <Td>{num(j.replayed_count)} / {num(j.estimated_count)}{j.failed_count ? ` (${j.failed_count} failed)` : ""}</Td>
                <Td className="text-xs text-muted">{j.requested_by}</Td>
              </Row>
            ))}
          </Table>
        )}
      </Card>
    </div>
  );
}
