import { Component, type ReactNode } from "react";
import { HashRouter, NavLink, Navigate, Route, Routes } from "react-router-dom";

import { PublicSourcesPage } from "./pages/PublicSourcesPage";
import { RssFeedPage } from "./pages/RssFeedPage";
import SalesHubPage from "./pages/SalesHubPage";

class ErrorBoundary extends Component<{ children: ReactNode }, { error: string | null }> {
  state = { error: null };
  static getDerivedStateFromError(err: unknown) {
    return { error: err instanceof Error ? err.message : String(err) };
  }
  render() {
    if (this.state.error) {
      return (
        <div className="panel error-panel" style={{ margin: 24 }}>
          <strong>Page crashed — check browser console for details.</strong>
          <pre style={{ whiteSpace: "pre-wrap", fontSize: "0.85rem" }}>{this.state.error}</pre>
          <button onClick={() => this.setState({ error: null })}>Try again</button>
        </div>
      );
    }
    return this.props.children;
  }
}

export default function App() {
  return (
    <HashRouter>
      <div className="app-shell">
        <header className="topnav">
          <div className="brand-inline">
            <span className="brand-kicker">Cold Lead Pipeline</span>
            <span className="brand-name">Signal Dashboard</span>
          </div>
          <nav className="nav-list">
            <NavLink
              className={({ isActive }) => `nav-link${isActive ? " nav-link-active" : ""}`}
              to="/rss-feed"
            >
              RSS Feed
            </NavLink>
            <NavLink
              className={({ isActive }) => `nav-link${isActive ? " nav-link-active" : ""}`}
              to="/public-sources"
            >
              Top Accounts
            </NavLink>
            <NavLink
              className={({ isActive }) => `nav-link${isActive ? " nav-link-active" : ""}`}
              to="/sales-hub"
            >
              Sales Hub
            </NavLink>
          </nav>
        </header>

        <main className="main-panel">
          <ErrorBoundary>
            <Routes>
              <Route path="/" element={<Navigate to="/rss-feed" replace />} />
              <Route path="/rss-feed" element={<RssFeedPage />} />
              <Route path="/public-sources" element={<PublicSourcesPage />} />
              <Route path="/sales-hub" element={<SalesHubPage />} />
            </Routes>
          </ErrorBoundary>
        </main>
      </div>
    </HashRouter>
  );
}
