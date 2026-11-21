import { formatAgo, formatAmount, formatCount, formatMs, formatPercent, formatRate, humanize } from "./format";

describe("format", () => {
  it("counts: full below 10k, compact above", () => {
    expect(formatCount(1284)).toBe("1,284");
    expect(formatCount(12_900)).toBe("12.9K");
    expect(formatCount(4_200_000)).toBe("4.2M");
    expect(formatCount(null)).toBe("—");
  });
  it("amounts from decimal strings", () => {
    expect(formatAmount("1234.5")).toBe("1,234.50");
    expect(formatAmount("295799.48")).toBe("295.8K");
    expect(formatAmount("295799.48", false)).toBe("295,799.48");
    expect(formatAmount("")).toBe("—");
  });
  it("percent, rate, ms", () => {
    expect(formatPercent(0.6777)).toBe("67.8%");
    expect(formatRate(90.717)).toBe("90.7");
    expect(formatRate(2.345)).toBe("2.35");
    expect(formatMs(4.25)).toBe("4.3 ms");
    expect(formatMs(850)).toBe("850 ms");
    expect(formatMs(1240)).toBe("1.24 s");
  });
  it("relative time and enum labels", () => {
    const now = 1_000_000;
    expect(formatAgo(now - 2_000, now)).toBe("just now");
    expect(formatAgo(now - 42_000, now)).toBe("42s ago");
    expect(formatAgo(now - 180_000, now)).toBe("3m ago");
    expect(humanize("PAYMENT_SUCCESS")).toBe("Payment success");
    expect(humanize("invalid_json")).toBe("Invalid json");
  });
});
