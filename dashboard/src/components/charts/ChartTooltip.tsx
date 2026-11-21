import type { TooltipProps } from "recharts";

export interface TooltipSeries {
  key: string;
  label: string;
  color: string;
  format: (v: number) => string;
}

/** One tooltip, every series at that X; value leads, label follows; line keys. */
export function makeTooltip(series: TooltipSeries[], titleOf: (x: number) => string) {
  return function ChartTooltip({ active, payload, label }: TooltipProps<number, string>) {
    if (!active || !payload?.length) return null;
    const row = payload[0].payload as Record<string, number>;
    return (
      <div className="tooltip">
        <div className="tooltip-title">{titleOf(Number(label))}</div>
        {series.map((s) => (
          <div className="tooltip-row" key={s.key}>
            <span>
              <span className="key-line" style={{ background: s.color }} aria-hidden />
              {s.label}
            </span>
            <strong>{row[s.key] == null ? "—" : s.format(row[s.key])}</strong>
          </div>
        ))}
      </div>
    );
  };
}
