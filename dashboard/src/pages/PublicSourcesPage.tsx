import { useEffect, useState, useMemo } from "react";

import { loadPublicSnapshot } from "../data";
import type { PublicSnapshot } from "../types";

function formatMoney(value: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value);
}

function formatPct(value: number | null): string {
  if (value === null || Number.isNaN(value)) {
    return "n/a";
  }
  return `${(value * 100).toFixed(1)}%`;
}

function formatDate(value: string): string {
  if (!value) {
    return "Unknown";
  }
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
          (acct.reason_to_contact ?? "").toLowerCase().includes(q) ||
          (acct.industry_segment ?? "").toLowerCase().includes(q)
        );
      }
      return true;
    });
  }, [data, regionFilter, priorityFilter, query]);

  if (state.error) {
    return <div className="panel error-panel">Public-source snapshot failed to load: {state.error}</div>;
  }

  if (!data) {
    return <div className="panel loading-panel">Loading public-source snapshot...</div>;
  }

  return (
    <section className="page-shell">
      <div className="hero-card hero-card-public">
        <div>
          <p className="eyebrow">Market Context</p>
          <h1>Public Sources</h1>
          <p className="hero-copy">
            Census, USAspending, and BLS-derived context tables that support account fit and
            sector-level demand sizing.
          </p>
        </div>
        <div className="stat-strip">
          <div className="stat-card">
            <span className="stat-label">NAICS Rows</span>
            <strong>{data.naics_enrichment.length}</strong>
          </div>
          <div className="stat-card">
            <span className="stat-label">BLS Segments</span>
            <strong>{data.bls_growth.length}</strong>
          </div>
          <div className="stat-card">
            <span className="stat-label">Snapshot</span>
            <strong>{formatDate(data.exported_at)}</strong>
          </div>
        </div>
      </div>

      <div className="content-grid">
        {data.top_accounts && data.top_accounts.length > 0 && (
          <section className="panel">
            <div className="panel-header">
              <h2>Top Accounts</h2>
              <p>Accounts within the tracked regions prioritized for sales follow-up.</p>
            </div>
            <div style={{ display: "flex", gap: 10, alignItems: "center", marginBottom: 12 }}>
              <label>
                Region:&nbsp;
                <select value={regionFilter} onChange={(e) => setRegionFilter(e.target.value)}>
                  {regions.map((r) => (
                    <option key={r} value={r}>
                      {r}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                Priority:&nbsp;
                <select value={priorityFilter} onChange={(e) => setPriorityFilter(e.target.value)}>
                  {priorities.map((p) => (
                    <option key={p} value={p}>
                      {p}
                    </option>
                  ))}
                </select>
              </label>
              <label style={{ flex: 1 }}>
                Search:&nbsp;
                <input
                  style={{ width: "100%" }}
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  placeholder="company, industry, or reason"
                />
              </label>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Account</th>
                    <th>Region</th>
                    <th>NAICS 2</th>
                    <th>Industry</th>
                    <th>Priority</th>
                    <th>Reason To Contact</th>
                    <th style={{ minWidth: 220 }}>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredAccounts.map((acct, idx) => (
                    <tr key={`${acct.account_name}-${idx}`}>
                      <td>
                        <strong>{acct.account_name}</strong>
                        <div style={{ color: "var(--muted)", fontSize: "0.85rem" }}>
                          {acct.industry_segment}
                        </div>
                      </td>
                      <td>{acct.region}</td>
                      <td>{acct.naics2}</td>
                      <td>{acct.industry_segment}</td>
                      <td>{acct.overall_sales_priority ?? "-"}</td>
                      <td>{acct.reason_to_contact ?? "-"}</td>
                      <td>
                        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
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
                              const q = encodeURIComponent(acct.account_name);
                              window.open(`https://www.linkedin.com/search/results/all/?keywords=${q}`, "_blank");
                            }}
                          >
                            Search LinkedIn
                          </button>
                          <button
                            onClick={() => {
                              if (acct.reason_to_contact) navigator.clipboard?.writeText(acct.reason_to_contact);
                            }}
                          >
                            Copy Reason
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        )}
        <section className="panel">
          <div className="panel-header">
            <h2>Source Freshness</h2>
            <p>Last-seen timestamps for the public-source tables feeding the dashboard.</p>
          </div>
          <div className="freshness-grid">
            {data.source_freshness.map((item) => (
              <article className="freshness-card" key={item.source_name}>
                <span className="stat-label">{item.source_name}</span>
                <strong>{item.record_count.toLocaleString()}</strong>
                <span>{formatDate(item.latest_timestamp)}</span>
              </article>
            ))}
          </div>
        </section>

        <section className="panel">
          <div className="panel-header">
            <h2>Top NAICS Enrichment</h2>
            <p>California workforce and federal-spend context ranked by external signal points.</p>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>NAICS 2</th>
                  <th>Employees</th>
                  <th>Payroll</th>
                  <th>Federal Amount</th>
                  <th>External Points</th>
                </tr>
              </thead>
              <tbody>
                {data.naics_enrichment.map((item) => (
                  <tr key={item.naics2}>
                    <td>{item.naics2}</td>
                    <td>{item.employees_ca.toLocaleString()}</td>
                    <td>{formatMoney(item.annual_payroll_ca)}</td>
                    <td>{formatMoney(item.federal_amount_ca)}</td>
                    <td>{item.external_signal_points}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>

        <section className="panel panel-wide">
          <div className="panel-header">
            <h2>BLS Segment Growth</h2>
            <p>Latest 12-month employment trend snapshot for the tracked California segments.</p>
          </div>
          <div className="growth-grid">
            {data.bls_growth.map((item) => (
              <article className="growth-card" key={item.segment}>
                <span className="stat-label">{item.segment}</span>
                <strong>{item.latest_value.toLocaleString()}</strong>
                <span>Prior 12m: {item.prior_12m_value.toLocaleString()}</span>
                <span className={item.pct_change_12m && item.pct_change_12m >= 0 ? "up" : "down"}>
                  {formatPct(item.pct_change_12m)}
                </span>
              </article>
            ))}
          </div>
        </section>
      </div>
    </section>
  );
}
