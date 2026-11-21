export interface LegendItem {
  label: string;
  color: string;
  shape: "line" | "rect";
}

/** Shown for 2+ series (identity is never color-alone). Text uses ink tokens. */
export function Legend({ items }: { items: LegendItem[] }) {
  if (items.length < 2) return null;
  return (
    <ul className="legend" aria-label="Legend">
      {items.map((i) => (
        <li key={i.label}>
          <span className={i.shape === "line" ? "key-line" : "key-rect"} style={{ background: i.color }} aria-hidden />
          {i.label}
        </li>
      ))}
    </ul>
  );
}
