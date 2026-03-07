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
            Regulatory headlines, targeted eye-safety news, and company-specific watchlist pulls
            from the feeds wired into the pipeline.
          </p>
        </div>
        <div className="stat-strip">
          <div className="stat-card">
            <span className="stat-label">Articles</span>
            <strong>{data.article_count}</strong>
          </div>
          <div className="stat-card">
            <span className="stat-label">Watchlist Matches</span>
            <strong>{data.watchlist_count}</strong>
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
            <h2>Company Watchlist</h2>
            <p>Matched articles for actionable eyewear accounts.</p>
          </div>
          {data.watchlist.length === 0 ? (
            <div className="empty-state">
              <strong>No company matches right now.</strong>
              <span>
                The feed is live. It will populate here as articles start matching the actionable
                eyewear account list.
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
                  {data.watchlist.map((item) => (
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
            <p>Recent RSS articles scored for eyewear relevance and urgency.</p>
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
                  <span>Relevance {article.eyewear_relevance_score}</span>
                  <span>Urgency {article.urgency_score}</span>
                  <span>{formatDate(article.article_published_at)}</span>
                </div>
              </article>
            ))}
          </div>
        </section>
      </div>
    </section>
  );
}
