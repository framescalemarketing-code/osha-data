import { useEffect, useState } from "react";

import { loadRssSnapshot } from "../data";
import type { RssSnapshot } from "../types";

function formatDate(value: string): string {
  if (!value) {
    return "Unknown";
  }
  return new Date(value).toLocaleString();
}

export function RssFeedPage() {
  const [state, setState] = useState<{ data: RssSnapshot | null; error: string | null }>({
    data: null,
    error: null,
  });

  useEffect(() => {
    void loadRssSnapshot(setState);
  }, []);

  if (state.error) {
    return <div className="panel error-panel">RSS snapshot failed to load: {state.error}</div>;
  }

  if (!state.data) {
    return <div className="panel loading-panel">Loading RSS feed snapshot...</div>;
  }

  const { data } = state;

  return (
    <section className="page-shell">
      <div className="hero-card hero-card-rss">
        <div>
          <p className="eyebrow">Current Awareness</p>
          <h1>RSS Newsfeed</h1>
          <p className="hero-copy">
            Industry news from the configured feeds, focused on expansion, acquisitions, funding,
            hiring, and other business signals that can support a more structured safety eyewear
            program conversation.
          </p>
        </div>
        <div className="stat-strip">
          <div className="stat-card">
            <span className="stat-label">Articles</span>
            <strong>{data.article_count}</strong>
          </div>
          <div className="stat-card">
            <span className="stat-label">Alignment Watchlist</span>
            <strong>{data.alignment_watchlist_count}</strong>
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
            <h2>Alignment Watchlist</h2>
            <p>Accounts where strict eyewear targets line up with RSS business momentum signals.</p>
          </div>
          {data.alignment_watchlist.length === 0 ? (
            <div className="empty-state">
              <strong>No aligned accounts right now.</strong>
              <span>
                The RSS feed is live. This section fills only when a strict eyewear target also
                appears in relevant business news from the industry feeds.
              </span>
            </div>
          ) : (
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
                  {data.alignment_watchlist.map((item) => (
                    <tr key={`${item["Account Name"]}-${item["Article Link"]}`}>
                      <td>{item["Account Name"]}</td>
                      <td>
                        <span className="pill">{item["Eyewear Need Tier"]}</span>
                      </td>
                      <td>
                        <a href={item["Article Link"]} target="_blank" rel="noreferrer">
                          {item["Article Title"]}
                        </a>
                      </td>
                      <td>{item["Feed Title"]}</td>
                      <td>{item["Article Priority"]}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>

        <section className="panel">
          <div className="panel-header">
            <h2>Latest Feed Items</h2>
            <p>Recent RSS articles scored for business opportunity signal and momentum.</p>
          </div>
          <div className="rss-card-list">
            {data.articles.map((article) => (
              <article className="rss-card" key={`${article.article_link}-${article.article_title}`}>
                <div className="rss-card-meta">
                  <span className="pill">{article.article_priority}</span>
                  <span>{article.feed_title}</span>
                </div>
                <h3>
                  <a href={article.article_link} target="_blank" rel="noreferrer">
                    {article.article_title}
                  </a>
                </h3>
                <div className="score-row">
                  <span>Signal {article.opportunity_signal_score}</span>
                  <span>Momentum {article.momentum_score}</span>
                  <span>{formatDate(article.article_published_at)}</span>
                </div>
                <p>{article.signal_summary}</p>
              </article>
            ))}
          </div>
        </section>
      </div>
    </section>
  );
}
