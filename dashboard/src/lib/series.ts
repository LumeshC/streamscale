// Time-series helpers shared by charts.

export type Row = { t: number } & Record<string, number | null>;

/** Smallest positive spacing between consecutive points (the series' step). */
export function inferStep(rows: { t: number }[]): number | null {
  let step: number | null = null;
  for (let i = 1; i < rows.length; i++) {
    const d = rows[i].t - rows[i - 1].t;
    if (d > 0 && (step === null || d < step)) step = d;
  }
  return step;
}

/**
 * Insert empty (null) points where the series skips steps, so lines break and
 * columns leave a hole instead of bridging periods with no data (e.g. while the
 * processor was down). Rows must be sorted oldest → newest.
 */
export function fillGaps(rows: Row[], keys: string[], stepMs: number | null = inferStep(rows)): Row[] {
  if (!stepMs || rows.length < 2) return rows;
  const out: Row[] = [];
  for (let i = 0; i < rows.length; i++) {
    if (i > 0) {
      for (let t = rows[i - 1].t + stepMs; t < rows[i].t - stepMs / 2; t += stepMs) {
        const empty: Row = { t };
        for (const k of keys) empty[k] = null;
        out.push(empty);
      }
    }
    out.push(rows[i]);
  }
  return out;
}
