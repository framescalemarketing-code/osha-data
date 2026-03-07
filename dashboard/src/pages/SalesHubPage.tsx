import { useEffect, useState } from "react";

import { loadPublicSnapshot, loadRssSnapshot } from "../data";
import type { PublicSnapshot, RssSnapshot } from "../types";

function formatDate(value: string): string {
  if (!value) return "Unknown";
  return new Date(value).toLocaleString();
}

export function SalesHubPage() {
  const [pubState, setPubState] = useState<{ data: PublicSnapshot | null; error: string | null }>({
    data: null,
    error: null,
  });
  const [rssState, setRssState] = useState<{ data: RssSnapshot | null; error: string | null }>({
    data: null,
    error: null,
  });

  useEffect(() => {
    void loadPublicSnapshot(setPubState);
    void loadRssSnapshot(setRssState);
  }, []);

  if (pubState.error) return <div className="panel error-panel">Public snapshot failed: {pubState.error}</div>;
  if (rssState.error) return <div className="panel error-panel">RSS snapshot failed: {rssState.error}</div>;
  if (!pubState.data || !rssState.data) return <div className="panel loading-panel">Loading sales hub...</div>;

  const top = pubState.data.top_accounts ?? [];
  const watch = rssState.data.alignment_watchlist ?? [];
  const watchNames = new Set(watch.map((w) => (w["Account Name"] || "").toUpperCase().trim()));
  const overlaps = top.filter((t) => watchNames.has((t.account_name || "").toUpperCase().trim()));

  return (
    <section className="page-shell">
      <div className="hero-card hero-card-public">
        <div>
          <p className="eyebrow">Sales Hub</p>
          <h1>Consolidated Sales View</h1>
          <p className="hero-copy">Top accounts, aligned news, and overlapping signals to prioritize outreach.</p>
        </div>
        <div className="stat-strip">
          <div className="stat-card">
            <span className="stat-label">Top Accounts</span>
            <strong>{top.length}</strong>
          </div>
          <div className="stat-card">
            <span className="stat-label">Aligned Accounts</span>
            <strong>{watch.length}</strong>
          </div>
          <div className="stat-card">
            <span className="stat-label">Overlaps</span>
            <strong>{overlaps.length}</strong>
          </div>
        </div>
      </div>

      <div className="content-grid">
        <section className="panel panel-wide">
          <div className="panel-header">
            <h2>Overlapping Targets</h2>
            <p>Companies that appear in both the public targets and the aligned RSS watchlist.</p>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Account</th>
                  <th>Region</th>
                  <th>NAICS</th>
                  <th>Priority</th>
                  <th>Reason</th>
                </tr>
              </thead>
              <tbody>
                {overlaps.map((a, i) => (
                  <tr key={`${a.account_name}-${i}`}>
                    <td>{a.account_name}</td>
                    <td>{a.region}</td>
                    <td>{a.naics2}</td>
                    <td>{a.overall_sales_priority ?? "-"}</td>
                    <td>{a.reason_to_contact ?? "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>

        <section className="panel panel-wide">
          <div className="panel-header">
            <h2>Top Accounts</h2>
            <p>All companies in the target summary for outreach.</p>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Account</th>
                  <th>Region</th>
                  <th>NAICS</th>
                  <th>Priority</th>
                  <th>Reason</th>
                </tr>
              </thead>
              <tbody>
                {top.map((a, i) => (
                  <tr key={`${a.account_name}-${i}`}>
                    <td>{a.account_name}</td>
                    <td>{a.region}</td>
                    <td>{a.naics2}</td>
                    <td>{a.overall_sales_priority ?? "-"}</td>
                    <td>{a.reason_to_contact ?? "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>

        <section className="panel panel-wide">
          <div className="panel-header">
            <h2>Alignment Watchlist (RSS)</h2>
            <p>Accounts surfaced by news alignment and their triggering article.</p>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Account</th>
                  <th>Tier</th>
                  <th>Article</th>
                  <th>Feed</th>
                  <th>Priority</th>
                </tr>
              </thead>
              <tbody>
                {watch.map((w, i) => (
                  <tr key={`${w["Account Name"]}-${i}`}>
                    <td>{w["Account Name"]}</td>
                    <td>
                      <span className="pill">{w["Eyewear Need Tier"]}</span>
                    </td>
                    <td>
                      <a href={w["Article Link"]} target="_blank" rel="noreferrer">
                        {w["Article Title"]}
                      </a>
                    </td>
                    <td>{w["Feed Title"]}</td>
                    <td>{w["Article Priority"]}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      </div>
    </section>
  );
}

export default SalesHubPage;
