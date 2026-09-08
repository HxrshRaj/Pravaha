"use client";

import {
  Area,
  AreaChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

type Point = Record<string, number | string>;

const axisStyle = { fontSize: 11, fill: "rgb(var(--muted))" };

export function TimeSeries({
  data,
  xKey = "t",
  series,
  height = 220,
  area = false,
  yFormat,
}: {
  data: Point[];
  xKey?: string;
  series: { key: string; label?: string; color?: string }[];
  height?: number;
  area?: boolean;
  yFormat?: (v: number) => string;
}) {
  const colors = [
    "rgb(var(--accent))",
    "rgb(var(--ok))",
    "rgb(var(--warn))",
    "rgb(var(--danger))",
  ];
  const Chart = area ? AreaChart : LineChart;
  return (
    <ResponsiveContainer width="100%" height={height}>
      <Chart data={data} margin={{ top: 6, right: 12, left: 4, bottom: 0 }}>
        <CartesianGrid stroke="rgb(var(--border))" strokeDasharray="3 3" vertical={false} />
        <XAxis
          dataKey={xKey}
          tick={axisStyle}
          tickLine={false}
          axisLine={{ stroke: "rgb(var(--border))" }}
          tickFormatter={(v) =>
            typeof v === "string" && v.includes("T")
              ? new Date(v).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
              : String(v)
          }
          minTickGap={40}
        />
        <YAxis
          tick={axisStyle}
          tickLine={false}
          axisLine={false}
          width={48}
          tickFormatter={(v) => (yFormat ? yFormat(Number(v)) : String(v))}
        />
        <Tooltip
          contentStyle={{
            background: "rgb(var(--surface))",
            border: "1px solid rgb(var(--border))",
            borderRadius: 8,
            fontSize: 12,
            color: "rgb(var(--fg))",
          }}
          labelFormatter={(v) =>
            typeof v === "string" && v.includes("T") ? new Date(v).toLocaleString() : String(v)
          }
        />
        {series.map((s, i) =>
          area ? (
            <Area
              key={s.key}
              type="monotone"
              dataKey={s.key}
              name={s.label ?? s.key}
              stroke={s.color ?? colors[i % colors.length]}
              fill={s.color ?? colors[i % colors.length]}
              fillOpacity={0.12}
              strokeWidth={1.8}
              dot={false}
              isAnimationActive={false}
            />
          ) : (
            <Line
              key={s.key}
              type="monotone"
              dataKey={s.key}
              name={s.label ?? s.key}
              stroke={s.color ?? colors[i % colors.length]}
              strokeWidth={1.8}
              dot={false}
              isAnimationActive={false}
            />
          ),
        )}
      </Chart>
    </ResponsiveContainer>
  );
}

export function Sparkline({
  data,
  dataKey = "value",
  height = 40,
  color = "rgb(var(--accent))",
}: {
  data: Point[];
  dataKey?: string;
  height?: number;
  color?: string;
}) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <LineChart data={data} margin={{ top: 2, right: 2, left: 2, bottom: 2 }}>
        <Line
          type="monotone"
          dataKey={dataKey}
          stroke={color}
          strokeWidth={1.5}
          dot={false}
          isAnimationActive={false}
        />
      </LineChart>
    </ResponsiveContainer>
  );
}
