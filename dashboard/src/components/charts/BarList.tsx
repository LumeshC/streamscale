import { formatCount, formatPercent } from "../../lib/format";

export interface BarListItem {
  key: string;
  label: string;
  value: number;
  share: number;
}

/**
 * Ranked horizontal bars for a distribution. One series → one color (slot 1),
 * value + share printed at each bar's tip, hover/focus reveals the exact count.
 */
export function BarList({ items, ariaLabel }: { items: BarListItem[]; ariaLabel: string }) {
  const max = Math.max(1, ...items.map((i) => i.value));
  return (
    <ul className="barlist" aria-label={ariaLabel}>
      {items.map((i) => (
        <li key={i.key} tabIndex={0} title={`${i.label}: ${i.value.toLocaleString("en-US")} events (${formatPercent(i.share, 2)})`}>
          <span className="name">{i.label}</span>
          <span className="track" aria-hidden>
            <span className="bar" style={{ width: `${(i.value / max) * 100}%` }} />
          </span>
          <span className="val">
            {formatCount(i.value)}
            <small>{formatPercent(i.share)}</small>
          </span>
        </li>
      ))}
    </ul>
  );
}
