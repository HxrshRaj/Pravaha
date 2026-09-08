"use client";

import { TimeSeries } from "@/components/charts";
import { PageHeader } from "@/components/shell";
import { Card, EmptyState, Spinner } from "@/components/ui";
import { num } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import { useSearchParams } from "next/navigation";
import { Suspense, useState } from "react";

function LagInner() {
  const sp = useSearchParams();
  const { data: overview } = useApi<{ groups: { group_name: string; lag: number }[] }>(
    "/api/v1/consumers/lag/overview",
    { intervalMs: 6000 },
  );
  const [group, setGroup] = useState(sp.get("group") ?? "pravaha.analytics");
  const { data } = useApi<{ points: { t: string; lag: number }[] }>(
    `/api/v1/consumers/lag?group_name=${encodeURIComponent(group)}&minutes=120`,
    { intervalMs: 6000, deps: [group] },
  );

  return (
    <div>
      <PageHeader title="Consumer Lag" desc="Committed offset vs log-end offset, summed over partitions, sampled every 15s." />
      <div className="grid grid-cols-2 gap-3 md:grid-cols-5">
        {(overview?.groups ?? []).map((g) => (
          <button
            key={g.group_name}
            onClick={() => setGroup(g.group_name)}
            className={`rounded-lg border px-3 py-2 text-left ${
              group === g.group_name ? "border-accent" : "border-border"
            }`}
          >
            <div className="truncate text-xs text-muted">{g.group_name}</div>
            <div className={`tabular text-lg font-semibold ${g.lag > 2000 ? "text-danger" : ""}`}>
              {num(g.lag)}
            </div>
          </button>
        ))}
      </div>
      <Card title={`${group} — lag over time`} className="mt-3">
        {!data ? (
          <Spinner />
        ) : data.points.length === 0 ? (
          <EmptyState>No lag samples yet.</EmptyState>
        ) : (
          <TimeSeries data={data.points} series={[{ key: "lag", label: "lag", color: "rgb(var(--danger))" }]} area />
        )}
      </Card>
    </div>
  );
}

export default function ConsumerLagPage() {
  return (
    <Suspense fallback={<Spinner />}>
      <LagInner />
    </Suspense>
  );
}
