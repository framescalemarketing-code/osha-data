import { HashRouter, NavLink, Navigate, Route, Routes } from "react-router-dom";

import { PublicSourcesPage } from "./pages/PublicSourcesPage";
import { RssFeedPage } from "./pages/RssFeedPage";

export default function App() {
  return (
    <HashRouter>
      <div className="app-shell">
        <aside className="sidebar">
          <div className="brand-block">
            <span className="brand-kicker">Cold Lead Pipeline</span>
            <h1>Signal Dashboard</h1>
            <p>Two focused views for current awareness and market context.</p>
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
              Public Sources
            </NavLink>
          </nav>
        </aside>

        <main className="main-panel">
          <Routes>
            <Route path="/" element={<Navigate to="/rss-feed" replace />} />
            <Route path="/rss-feed" element={<RssFeedPage />} />
            <Route path="/public-sources" element={<PublicSourcesPage />} />
          </Routes>
        </main>
      </div>
    </HashRouter>
  );
}
