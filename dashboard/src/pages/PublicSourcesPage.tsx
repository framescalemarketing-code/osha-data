import { useEffect, useState, useMemo } from "react";

import { loadPublicSnapshot } from "../data";
import type { PublicSnapshot } from "../types";

function formatDate(value: string): string {
  if (!value) return "Unknown";
  return new Date(value).toLocaleString();
}

export function PublicSourcesPage() {
  const [state, setState] = useState<{ data: PublicSnapshot | null; error: string | null }>({
    data: null,
    error: null,
  });
  const [regionFilter, setRegionFilter] = useState<string>("All");
  const [priorityFilter, setPriorityFilter] = useState<string>("All");
  const [query, setQuery] = useState<string>("");

  useEffect(() => {
    void loadPublicSnapshot(setState);
  }, []);

  // All hooks must run unconditionally before any early return (Rules of Hooks).
  const data = state.data;

  const regions = useMemo(
    () => ["All", ...(data?.regions ?? [])],
    [data],
  );

  const priorities = useMemo(() => {
    const set = new Set<string>();
    (data?.top_accounts ?? []).forEach(
      (a) => a.overall_sales_priority && set.add(a.overall_sales_priority),
    );
    return ["All", ...Array.from(set)];
  }, [data]);

  const filteredAccounts = useMemo(() => {
    if (!data?.top_accounts) return [];
    return data.top_accounts.filter((acct) => {
      if (regionFilter !== "All" && acct.region !== regionFilter) return false;
      if (priorityFilter !== "All" && (acct.overall_sales_priority ?? "") !== priorityFilter) return false;
      if (query) {
        const q = query.toLowerCase();
        return (
          (acct.account_name ?? "").toLowerCase().includes(q) ||
          (acct.recent_inspection_context ?? "").toLowerCase().includes(q) ||
          (acct.overall_history ?? "").toLowerCase().includes(q) ||
          (acct.reason_to_contact ?? "").toLowerCase().includes(q) ||
          (acct.industry_segment ?? "").toLowerCase().includes(q)
        );
      }
      return true;
    });
  }, [data, regionFilter, priorityFilter, query]);

  if (state.error) {
    return <div className="panel error-panel">Failed to load accounts: {state.error}</div>;
  }

  if (!data) {
    return <div className="panel loading-panel">Loading accounts...</div>;
  }

  const total = (data.top_accounts ?? []).length;

  return (
    <section className="page-shell">
      <div className="page-header-row">
        <div>
          <h1 className="page-title">Top Accounts</h1>
          <p className="page-subtitle">
            {filteredAccounts.length} of {total} accounts &mdash; San Diego &amp; Bay Area, sorted by sales priority
          </p>
        </div>
        <span className="snapshot-badge">Updated {formatDate(data.exported_at)}</span>
      </div>

      <div className="filter-row">
        <label>
          Region&nbsp;
          <select value={regionFilter} onChange={(e) => setRegionFilter(e.target.value)}>
            {regions.map((r) => <option key={r} value={r}>{r}</option>)}
          </select>
        </label>
        <label>
          Priority&nbsp;
          <select value={priorityFilter} onChange={(e) => setPriorityFilter(e.target.value)}>
            {priorities.map((p) => <option key={p} value={p}>{p}</option>)}
          </select>
        </label>
        <label className="search-label">
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search company, industry, or reason..."
          />
        </label>
      </div>

      <div className="panel panel-wide">
        {filteredAccounts.length === 0 ? (
          <p className="empty-state">No accounts match the current filters.</p>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Account</th>
                  <th>Region</th>
                  <th>Priority</th>
                  <th>Recent Inspection</th>
                  <th>Overall History</th>
                  <th>Reason To Contact</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {filteredAccounts.map((acct, idx) => (
                  <tr key={`${acct.account_name}-${idx}`}>
                    <td>
                      <strong>{acct.account_name}</strong>
                      {acct.industry_segment && (
                        <div style={{ color: "var(--muted)", fontSize: "0.82rem", marginTop: 2 }}>
                          {acct.industry_segment}
                        </div>
                      )}
                    </td>
                    <td style={{ whiteSpace: "nowrap" }}>{acct.region}</td>
                    <td style={{ whiteSpace: "nowrap" }}>
                      {acct.overall_sales_priority
                        ? <span className="pill">{acct.overall_sales_priority}</span>
                        : <span style={{ color: "var(--muted)" }}>&mdash;</span>}
                    </td>
                    <td style={{ maxWidth: 280 }}>{acct.recent_inspection_context ?? "-"}</td>
                    <td style={{ maxWidth: 280 }}>{acct.overall_history ?? "-"}</td>
                    <td style={{ maxWidth: 280 }}>{acct.reason_to_contact ?? "-"}</td>
                    <td>
                      <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                        <button
                          onClick={() => {
                            const q = encodeURIComponent(`${acct.account_name} safety eyewear PPE`);
                            window.open(`https://www.google.com/search?q=${q}`, "_blank");
                          }}
                        >
                          Search Web
                        </button>
                        <button
                          onClick={() => {
                            const q = encodeURIComponent(`site:linkedin.com/company "${acct.account_name}"`);
                            window.open(`https://www.google.com/search?q=${q}`, "_blank");
                          }}
                        >
                          Find on LinkedIn
                        </button>
                        {(acct.recent_inspection_context || acct.overall_history || acct.reason_to_contact) && (
                          <button
                            onClick={() => {
                              const parts = [
                                acct.recent_inspection_context && `Recent: ${acct.recent_inspection_context}`,
                                acct.overall_history && `History: ${acct.overall_history}`,
                                acct.reason_to_contact && `Contact: ${acct.reason_to_contact}`,
                              ].filter(Boolean).join("\n");
                              navigator.clipboard?.writeText(parts);
                            }}
                          >
                            Copy
                          </button>
                        )}
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="freshness-footer">
        <span className="stat-label">Data sources</span>
        <div className="freshness-strip">
          {data.source_freshness.map((item) => (
            <div className="freshness-chip" key={item.source_name}>
              <span className="freshness-name">
                {item.source_name.toLowerCase().replace(/_current$/, "").replace(/_/g, " ")}
              </span>
              <span className="freshness-count">{item.record_count.toLocaleString()} rows</span>
              <span className="freshness-ts">{formatDate(item.latest_timestamp)}</span>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
