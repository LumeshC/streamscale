import { Line, LineChart, ResponsiveContainer, YAxis } from "recharts";

/** Tiny trend line (no axes); for session-local history such as lag samples. */
export function Sparkline({ values, ariaLabel, height = 44 }: { values: number[]; ariaLabel: string; height?: number }) {
  const data = values.map((v, i) => ({ i, v }));
  return (
    <div style={{ width: "100%", height }} role="img" aria-label={ariaLabel}>
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 4, right: 4, bottom: 4, left: 4 }}>
          <YAxis hide domain={[0, "dataMax"]} />
          <Line dataKey="v" stroke="var(--series-1)" strokeWidth={2} dot={false} isAnimationActive={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
