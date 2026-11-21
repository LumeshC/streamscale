import { mockFetch } from "../test-utils";
import { ApiError, buildUrl, getAllPages, getJSON } from "./client";

afterEach(() => vi.unstubAllGlobals());

describe("buildUrl", () => {
  it("prefixes /api and drops empty params", () => {
    expect(buildUrl("/metrics/events", { interval: "1m", cursor: undefined, x: "" })).toBe("/api/metrics/events?interval=1m");
  });
});

describe("getJSON", () => {
  it("returns parsed JSON", async () => {
    mockFetch(() => ({ body: { status: "ok" } }));
    await expect(getJSON("/health")).resolves.toEqual({ status: "ok" });
  });

  it("maps the API error envelope to ApiError", async () => {
    mockFetch(() => ({
      status: 400,
      body: { error: { code: "invalid_parameter", message: "bad", details: [{ field: "from", reason: "x" }] }, request_id: "abc" },
    }));
    const err = (await getJSON("/metrics/events").catch((e) => e)) as ApiError;
    expect(err).toBeInstanceOf(ApiError);
    expect(err).toMatchObject({ status: 400, code: "invalid_parameter", requestId: "abc" });
    expect(err.details?.[0].field).toBe("from");
  });

  it("treats proxy 502 without an envelope as API unreachable", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response("<html>Bad Gateway</html>", { status: 502 })));
    const err = (await getJSON("/health").catch((e) => e)) as ApiError;
    expect(err).toMatchObject({ status: 502, code: "network" });
    expect(err.userMessage).toMatch(/Cannot reach/);
  });

  it("maps network failure", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => { throw new TypeError("Failed to fetch"); }));
    await expect(getJSON("/health")).rejects.toMatchObject({ status: 0, code: "network" });
  });

  it("propagates caller aborts untouched (not as an ApiError)", async () => {
    vi.stubGlobal("fetch", vi.fn((_: unknown, init: RequestInit) => new Promise((_r, rej) => {
      init.signal!.addEventListener("abort", () => rej(new DOMException("aborted", "AbortError")));
    })));
    const ctrl = new AbortController();
    const p = getJSON("/health", {}, ctrl.signal);
    ctrl.abort();
    const err = (await p.catch((e) => e)) as Error;
    expect(err).not.toBeInstanceOf(ApiError);
    expect(err.name).toBe("AbortError");
  });
});

describe("getAllPages", () => {
  it("follows next_cursor and concatenates", async () => {
    const { calls } = mockFetch((u) => {
      const c = u.searchParams.get("cursor");
      if (!c) return { body: { data: [1, 2], page: { limit: 2, next_cursor: "p2" } } };
      if (c === "p2") return { body: { data: [3, 4], page: { limit: 2, next_cursor: "p3" } } };
      return { body: { data: [5], page: { limit: 2 } } };
    });
    const r = await getAllPages<number, { data: number[]; page: { limit: number; next_cursor?: string } }>("/metrics/events", { limit: 2 });
    expect(r.data).toEqual([1, 2, 3, 4, 5]);
    expect(r.truncated).toBe(false);
    expect(calls).toHaveLength(3);
    expect(calls[1].searchParams.get("limit")).toBe("2");
  });

  it("stops at maxPages and reports truncation", async () => {
    mockFetch(() => ({ body: { data: [1], page: { limit: 1, next_cursor: "more" } } }));
    const r = await getAllPages("/metrics/events", {}, undefined, 3);
    expect(r.data).toHaveLength(3);
    expect(r.truncated).toBe(true);
  });
});
