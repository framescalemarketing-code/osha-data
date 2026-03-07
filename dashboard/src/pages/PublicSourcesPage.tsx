import { useEffect, useState } from "react";

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

  useEffect(() => {
    void loadPublicSnapshot(setState);
  }, []);

  if (state.error) {
    return <div className="panel error-panel">Public-source snapshot failed to load: {state.error}</div>;
  }

  if (!state.data) {
    return <div className="panel loading-panel">Loading public-source snapshot...</div>;
  }

  const { data } = state;

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
