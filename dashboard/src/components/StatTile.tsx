import type { ReactNode } from "react";

interface StatTileProps {
  label: string;
  value: ReactNode;
  sub?: ReactNode;
  hero?: boolean;
}

/** label · value · optional context line. Values are pre-formatted. */
export function StatTile({ label, value, sub, hero }: StatTileProps) {
  return (
    <div className="tile">
      <div className="tile-label" title={label}>
        {label}
      </div>
      <div className={`tile-value${hero ? " hero" : ""}`}>{value}</div>
      {sub ? <div className="tile-sub">{sub}</div> : null}
    </div>
  );
}
