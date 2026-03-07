import Link from "next/link";
import type { ReactNode } from "react";

const NAV_ITEMS = [
  { href: "/", label: "Overview" },
  { href: "/current", label: "Current Signals" },
  { href: "/opportunities", label: "RSS Opportunities" },
  { href: "/review", label: "Review Queue" },
  { href: "/intersections", label: "Intersections" },
];

export function DashboardShell({
  title,
  eyebrow,
  children,
}: {
  title: string;
  eyebrow: string;
  children: ReactNode;
}) {
  return (
    <div className="page-shell">
      <aside className="sidebar">
        <div className="brand-block">
          <p className="brand-kicker">Sales Intel</p>
          <h1>Operational signal board</h1>
          <p className="brand-copy">
            Current repo outputs stay separate from RSS expansion signals. Intersections raise the alert level.
          </p>
        </div>
        <nav className="nav-stack">
          {NAV_ITEMS.map((item) => (
            <Link key={item.href} href={item.href} className="nav-link">
              {item.label}
            </Link>
          ))}
        </nav>
      </aside>
      <main className="content-shell">
        <header className="page-header">
          <p className="eyebrow">{eyebrow}</p>
          <h2>{title}</h2>
        </header>
        {children}
      </main>
    </div>
  );
}
