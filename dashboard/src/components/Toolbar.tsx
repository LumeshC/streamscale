import { useEffect, useState } from "react";
import { RANGE_PRESETS, REFRESH_OPTIONS, useDashboard } from "../hooks/useDashboard";
import { Moon, Refresh, Sun } from "./Icons";

type Theme = "system" | "light" | "dark";

function applyTheme(t: Theme) {
  const root = document.documentElement;
  if (t === "system") delete root.dataset.theme;
  else root.dataset.theme = t;
}

/** One row of controls above the content; they scope every section. */
export function Toolbar() {
  const { range, setRangeId, refreshMs, setRefreshMs, refreshNow } = useDashboard();
  const [theme, setTheme] = useState<Theme>(() => {
    try {
      return (localStorage.getItem("ss.theme") as Theme) || "system";
    } catch {
      return "system";
    }
  });
  useEffect(() => {
    applyTheme(theme);
    try {
      localStorage.setItem("ss.theme", theme);
    } catch {
      /* ignore */
    }
  }, [theme]);

  const nextTheme: Theme = theme === "system" ? "dark" : theme === "dark" ? "light" : "system";
  const isDark =
    theme === "dark" || (theme === "system" && typeof matchMedia !== "undefined" && matchMedia("(prefers-color-scheme: dark)").matches);

  return (
    <div className="toolbar">
      <div className="segmented" role="group" aria-label="Time range">
        {RANGE_PRESETS.map((p) => (
          <button key={p.id} aria-pressed={p.id === range.id} onClick={() => setRangeId(p.id)} title={p.label}>
            {p.id}
          </button>
        ))}
      </div>
      <label>
        <span className="sr-only">Auto refresh</span>
        <select className="select" value={refreshMs} onChange={(e) => setRefreshMs(Number(e.target.value))} title="Auto refresh">
          {REFRESH_OPTIONS.map((o) => (
            <option key={o.ms} value={o.ms}>
              {o.ms ? `Refresh: ${o.label}` : "Refresh: off"}
            </option>
          ))}
        </select>
      </label>
      <button className="btn" onClick={refreshNow} title="Refresh now">
        <Refresh /> <span>Refresh</span>
      </button>
      <button className="btn btn-ghost" onClick={() => setTheme(nextTheme)} title={`Theme: ${theme} (switch to ${nextTheme})`} aria-label={`Theme: ${theme}`}>
        {isDark ? <Moon /> : <Sun />}
      </button>
    </div>
  );
}
