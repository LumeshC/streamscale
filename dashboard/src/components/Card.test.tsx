import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ApiError } from "../api/client";
import type { PollState } from "../hooks/usePolling";
import { AsyncBody, Card } from "./Card";

const base: PollState<number[]> = { data: undefined, error: undefined, loading: false, refreshing: false, updatedAt: null, retry: () => {} };

function renderBody(state: Partial<PollState<number[]>>) {
  return render(
    <Card title="T">
      <AsyncBody state={{ ...base, ...state }} isEmpty={(d) => d.length === 0} empty={{ title: "Nothing here" }}>
        {(d) => <p>rows: {d.length}</p>}
      </AsyncBody>
    </Card>,
  );
}

describe("AsyncBody", () => {
  it("shows a loading skeleton first", () => {
    renderBody({ loading: true });
    expect(screen.getByRole("status")).toHaveTextContent(/loading/i);
  });

  it("shows an error with request id and retry when there is no data", async () => {
    const retry = vi.fn();
    renderBody({ error: new ApiError("database down", 503, "unavailable", "rid-1"), retry });
    expect(screen.getByRole("alert")).toHaveTextContent("A backend dependency is unavailable.");
    expect(screen.getByText(/rid-1/)).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: /retry/i }));
    expect(retry).toHaveBeenCalled();
  });

  it("keeps stale data visible with a note when a refresh fails", () => {
    renderBody({ data: [1, 2], error: new ApiError("t", 504, "timeout") });
    expect(screen.getByText("rows: 2")).toBeInTheDocument();
    expect(screen.getByText(/showing last good data/i)).toBeInTheDocument();
  });

  it("shows the empty state", () => {
    renderBody({ data: [] });
    expect(screen.getByText("Nothing here")).toBeInTheDocument();
  });

  it("marks the body busy while refreshing", () => {
    const { container } = renderBody({ data: [1], refreshing: true });
    expect(container.querySelector(".card-body")).toHaveAttribute("aria-busy", "true");
  });
});
