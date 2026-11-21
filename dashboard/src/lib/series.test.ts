import { fillGaps, inferStep } from "./series";

const m = 60_000;

describe("fillGaps", () => {
  it("inserts null points for missing steps", () => {
    const rows = [{ t: 0, v: 1 }, { t: m, v: 2 }, { t: 4 * m, v: 3 }];
    expect(inferStep(rows)).toBe(m);
    const out = fillGaps(rows, ["v"]);
    expect(out.map((r) => r.t)).toEqual([0, m, 2 * m, 3 * m, 4 * m]);
    expect(out[2].v).toBeNull();
    expect(out[3].v).toBeNull();
  });
  it("leaves contiguous or tiny series alone", () => {
    const rows = [{ t: 0, v: 1 }, { t: m, v: 2 }];
    expect(fillGaps(rows, ["v"])).toEqual(rows);
    expect(fillGaps([{ t: 0, v: 1 }], ["v"])).toHaveLength(1);
  });
});
