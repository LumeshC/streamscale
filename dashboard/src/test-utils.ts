// Test helper: route fetch() by API path to canned JSON responses.
type Handler = (url: URL) => { status?: number; body: unknown; headers?: Record<string, string> } | undefined;

export function mockFetch(handler: Handler) {
  const calls: URL[] = [];
  const fn = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(String(input), "http://localhost");
    calls.push(url);
    if (init?.signal?.aborted) throw new DOMException("aborted", "AbortError");
    const r = handler(url);
    if (!r) return new Response(JSON.stringify({ error: { code: "not_found", message: "no route" } }), { status: 404 });
    return new Response(JSON.stringify(r.body), {
      status: r.status ?? 200,
      headers: { "Content-Type": "application/json", "X-Request-ID": "req-test", ...(r.headers ?? {}) },
    });
  });
  vi.stubGlobal("fetch", fn);
  return { fn, calls };
}
