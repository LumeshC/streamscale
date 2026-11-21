import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { formatTime } from "../../lib/format";
import type { Row } from "../../lib/series";
import { makeTooltip } from "./ChartTooltip";
import { Legend } from "./Legend";

export interface CSeries {
  key: string;
  label: string;
  color: string;
  format: (v: number) => string;
}

export interface ColumnChartProps {
  /** Rows oldest → newest; null values leave an empty slot (no data). */
  data: Row[];
  series: CSeries[]; // bottom → top when stacked
  stacked?: boolean;
  yTick: (v: number) => string;
  bucketLabel: (t: number) => string;
  height?: number;
  ariaLabel: string;
}

/**
 * Columns over time: ≤24px thick, 4px rounded data-end (top segment only when
 * stacked), square at the baseline, 2px surface-colored gap between segments.
 */
export function ColumnChart({ data, series, stacked, yTick, bucketLabel, height = 240, ariaLabel }: ColumnChartProps) {
  const Tip = makeTooltip(series, bucketLabel);
  return (
    <div>
      <Legend items={series.map((s) => ({ label: s.label, color: s.color, shape: "rect" }))} />
      <div className="chart" style={{ height }} role="img" aria-label={ariaLabel}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} margin={{ top: 6, right: 8, bottom: 0, left: 0 }} barCategoryGap="20%">
            <CartesianGrid vertical={false} stroke="var(--grid)" strokeWidth={1} />
            <XAxis
              dataKey="t"
              tickFormatter={(v) => formatTime(new Date(v).toISOString())}
              stroke="var(--axis)"
              tickLine={false}
              minTickGap={40}
            />
            <YAxis tickFormatter={yTick} stroke="var(--axis)" axisLine={false} tickLine={false} width={52} />
            <Tooltip content={<Tip />} cursor={{ fill: "var(--wash)" }} isAnimationActive={false} />
            {series.map((s, i) => (
              <Bar
                key={s.key}
                dataKey={s.key}
                name={s.label}
                stackId={stacked ? "s" : undefined}
                fill={s.color}
                maxBarSize={24}
                radius={!stacked || i === series.length - 1 ? [4, 4, 0, 0] : 0}
                stroke={stacked ? "var(--surface)" : undefined}
                strokeWidth={stacked ? 2 : 0}
                isAnimationActive={false}
              />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

/**
 * Sum per-minute rows into at most `maxBars` equal buckets (counts and amounts
 * are additive; rates must be recomputed by the caller from the sums).
 */
export function bucketize(rows: Row[], maxBars: number, sumKeys: string[]): { rows: Row[]; minutesPerBar: number } {
  if (rows.length <= maxBars) return { rows, minutesPerBar: 1 };
  const per = Math.ceil(rows.length / maxBars);
  const out: Row[] = [];
  for (let i = 0; i < rows.length; i += per) {
    const chunk = rows.slice(i, i + per);
    const acc: Row = { t: chunk[0].t };
    for (const k of sumKeys) {
      const vals = chunk.map((r) => r[k]).filter((v): v is number => v != null);
      acc[k] = vals.length ? vals.reduce((a, b) => a + b, 0) : null; // all-empty bucket stays empty
    }
    out.push(acc);
  }
  return { rows: out, minutesPerBar: per };
}
