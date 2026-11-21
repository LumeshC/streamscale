import { Area, CartesianGrid, ComposedChart, Line, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { formatTime } from "../../lib/format";
import { fillGaps, type Row } from "../../lib/series";
import { makeTooltip } from "./ChartTooltip";
import { Legend } from "./Legend";

export interface TSeries {
  key: string;
  label: string;
  color: string; // CSS color, e.g. "var(--series-1)"
  area?: boolean; // 10% wash under the line (single-series charts)
  format: (v: number) => string;
}

export interface TimeSeriesChartProps {
  /** Rows sorted oldest → newest; `t` is epoch ms. Gaps render as breaks. */
  data: Row[];
  series: TSeries[];
  yTick: (v: number) => string;
  seconds?: boolean;
  height?: number;
  ariaLabel: string;
}

/** Line/area chart over time: 2px lines, hairline grid, crosshair tooltip. */
export function TimeSeriesChart({ data, series, yTick, seconds, height = 240, ariaLabel }: TimeSeriesChartProps) {
  const Tip = makeTooltip(series, (x) => formatTime(new Date(x).toISOString(), seconds));
  const rows = fillGaps(data, series.map((s) => s.key));
  // A point with no neighbours draws no line segment; mark it with a dot
  // (r=4, surface ring) so isolated data stays visible.
  const isolatedDot = (key: string, color: string) =>
    function IsolatedDot(props: { cx?: number; cy?: number; index?: number }) {
      const i = props.index ?? -1;
      const prev = rows[i - 1]?.[key];
      const next = rows[i + 1]?.[key];
      if (rows[i]?.[key] == null || prev != null || next != null || props.cx == null || props.cy == null) {
        return <g key={`d-${key}-${i}`} />;
      }
      return <circle key={`d-${key}-${i}`} cx={props.cx} cy={props.cy} r={4} fill={color} stroke="var(--surface)" strokeWidth={2} />;
    };
  return (
    <div>
      <Legend items={series.map((s) => ({ label: s.label, color: s.color, shape: "line" }))} />
      <div className="chart" style={{ height }} role="img" aria-label={ariaLabel}>
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={rows} margin={{ top: 6, right: 8, bottom: 0, left: 0 }}>
            <CartesianGrid vertical={false} stroke="var(--grid)" strokeWidth={1} />
            <XAxis
              dataKey="t"
              type="number"
              scale="time"
              domain={["dataMin", "dataMax"]}
              tickFormatter={(v) => formatTime(new Date(v).toISOString(), seconds)}
              stroke="var(--axis)"
              tickLine={false}
              minTickGap={40}
            />
            <YAxis
              tickFormatter={yTick}
              stroke="var(--axis)"
              axisLine={false}
              tickLine={false}
              width={52}
              allowDecimals={false}
            />
            <Tooltip content={<Tip />} cursor={{ stroke: "var(--axis)", strokeWidth: 1 }} isAnimationActive={false} />
            {series.map((s) =>
              s.area ? (
                <Area
                  key={s.key}
                  dataKey={s.key}
                  name={s.label}
                  type="monotone"
                  stroke={s.color}
                  strokeWidth={2}
                  fill={s.color}
                  fillOpacity={0.1}
                  dot={isolatedDot(s.key, s.color)}
                  activeDot={{ r: 4, stroke: "var(--surface)", strokeWidth: 2 }}
                  isAnimationActive={false}
                />
              ) : (
                <Line
                  key={s.key}
                  dataKey={s.key}
                  name={s.label}
                  type="monotone"
                  stroke={s.color}
                  strokeWidth={2}
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  dot={isolatedDot(s.key, s.color)}
                  activeDot={{ r: 4, stroke: "var(--surface)", strokeWidth: 2 }}
                  isAnimationActive={false}
                />
              ),
            )}
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
