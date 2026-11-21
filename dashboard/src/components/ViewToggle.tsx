import { ChartIcon, TableIcon } from "./Icons";

export type View = "chart" | "table";

/** Chart ↔ table switch: every chart's values are reachable without hover. */
export function ViewToggle({ view, onChange, label }: { view: View; onChange: (v: View) => void; label: string }) {
  return (
    <div className="segmented" role="group" aria-label={`${label} view`}>
      <button aria-pressed={view === "chart"} onClick={() => onChange("chart")} title="Chart view">
        <ChartIcon />
        <span className="sr-only">Chart</span>
      </button>
      <button aria-pressed={view === "table"} onClick={() => onChange("table")} title="Table view">
        <TableIcon />
        <span className="sr-only">Table</span>
      </button>
    </div>
  );
}
