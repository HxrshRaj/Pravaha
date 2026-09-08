"use client";

import { PageHeader } from "@/components/shell";
import { Badge, Card, EmptyState } from "@/components/ui";
import { ago, shortId } from "@/lib/format";
import { useSSE } from "@/lib/hooks";
import Link from "next/link";
import { useMemo, useState } from "react";

type LiveEvent = {
  event_id: string;
  event_type: string;
  producer: string;
  region: string | null;
  event_time: string;
  correlation_id: string;
  partition: number;
  offset: number;
  payload: Record<string, unknown>;
};

export default function LiveStreamPage() {
  const [typeFilter, setTypeFilter] = useState("");
  const [search, setSearch] = useState("");
  const path = typeFilter
    ? `/api/v1/live/events?event_type=${encodeURIComponent(typeFilter)}`
    : "/api/v1/live/events";
  const { items, connected, paused, setPaused, clear } = useSSE<LiveEvent>(
    path,
    "event",
    { max: 300 },
  );
  const [selected, setSelected] = useState<LiveEvent | null>(null);

  const filtered = useMemo(() => {
    if (!search) return items;
    const q = search.toLowerCase();
    return items.filter(
      (e) =>
        e.event_type.toLowerCase().includes(q) ||
        e.correlation_id.toLowerCase().includes(q) ||
        (e.region ?? "").toLowerCase().includes(q) ||
        JSON.stringify(e.payload).toLowerCase().includes(q),
    );
  }, [items, search]);

  return (
    <div>
      <PageHeader
        title="Live Stream"
        desc="Real-time tail of events.validated over SSE. Bounded to 300 rows in the DOM."
        right={
          <div className="flex items-center gap-2">
            <Badge tone={connected ? "ok" : "danger"}>{connected ? "connected" : "reconnecting"}</Badge>
            <button
              className="rounded border border-border px-2 py-1 text-xs hover:bg-surface2"
              onClick={() => setPaused(!paused)}
            >
              {paused ? "Resume" : "Pause"}
            </button>
            <button
              className="rounded border border-border px-2 py-1 text-xs hover:bg-surface2"
              onClick={clear}
            >
              Clear
            </button>
          </div>
        }
      />

      <div className="mb-3 flex flex-wrap gap-2">
        <input
          placeholder="event_type filter (server-side)"
          className="w-56 rounded border border-border bg-bg px-2 py-1 text-sm outline-none focus:border-accent"
          value={typeFilter}
          onChange={(e) => setTypeFilter(e.target.value.trim())}
        />
        <input
          placeholder="search (client-side)"
          className="w-56 rounded border border-border bg-bg px-2 py-1 text-sm outline-none focus:border-accent"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
        <span className="self-center text-xs text-muted">
          {filtered.length} shown {paused && "· paused"}
        </span>
      </div>

      <div className="grid gap-3 lg:grid-cols-[1fr_360px]">
        <Card title="Events">
          {filtered.length === 0 ? (
            <EmptyState>
              Waiting for events… run the generator: <code>python -m pravaha.scripts.generate_events --bootstrap --rate 40 --duration 120</code>
            </EmptyState>
          ) : (
            <div className="max-h-[70vh] overflow-y-auto font-mono text-xs">
              <table className="w-full">
                <tbody>
                  {filtered.map((e) => (
                    <tr
                      key={e.event_id + e.offset}
                      onClick={() => setSelected(e)}
                      className="cursor-pointer border-b border-border/50 hover:bg-surface2"
                    >
                      <td className="whitespace-nowrap py-1 pr-2 text-muted">
                        {new Date(e.event_time).toLocaleTimeString()}
                      </td>
                      <td className="py-1 pr-2">
                        <span className="rounded bg-accent/10 px-1 text-accent">{e.event_type}</span>
                      </td>
                      <td className="py-1 pr-2 text-muted">{e.region}</td>
                      <td className="py-1 pr-2">p{e.partition}/{e.offset}</td>
                      <td className="py-1 text-muted">{shortId(e.correlation_id, 14)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Card>

        <Card title="Event detail">
          {selected ? (
            <div className="space-y-2 text-xs">
              <KV k="event_id" v={selected.event_id} />
              <KV k="type" v={selected.event_type} />
              <KV k="producer" v={selected.producer} />
              <KV k="region" v={selected.region ?? "—"} />
              <KV k="event_time" v={new Date(selected.event_time).toLocaleString()} />
              <KV k="partition / offset" v={`${selected.partition} / ${selected.offset}`} />
              <div>
                <div className="mb-1 text-muted">correlation_id</div>
                <Link
                  href={`/events?correlation_id=${selected.correlation_id}`}
                  className="break-all font-mono text-accent hover:underline"
                >
                  {selected.correlation_id}
                </Link>
              </div>
              <div>
                <div className="mb-1 text-muted">payload</div>
                <pre className="max-h-72 overflow-auto rounded bg-bg p-2 font-mono">
                  {JSON.stringify(selected.payload, null, 2)}
                </pre>
              </div>
            </div>
          ) : (
            <EmptyState>Select an event.</EmptyState>
          )}
        </Card>
      </div>
    </div>
  );
}

function KV({ k, v }: { k: string; v: string }) {
  return (
    <div className="flex justify-between gap-2">
      <span className="text-muted">{k}</span>
      <span className="break-all text-right font-mono">{v}</span>
    </div>
  );
}
