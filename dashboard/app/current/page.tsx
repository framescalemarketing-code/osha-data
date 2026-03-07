import { DashboardShell } from "@/components/dashboard-shell";
import { EmptyState } from "@/components/empty-state";
import { getCurrentSignals, getIntersections } from "@/lib/data";

export default async function CurrentSignalsPage() {
  const [signals, intersections] = await Promise.all([
    getCurrentSignals(),
    getIntersections(),
  ]);
  const intersectionCompanies = new Set(
    intersections.map((alert) => alert.companyNameNormalized),
  );

  return (
    <DashboardShell
      eyebrow="Current Pipeline View"
      title="Sales-friendly current repo outputs"
    >
      {signals.length === 0 ? (
        <EmptyState
          title="No current signal snapshot found"
          body="Run `python -m pipeline.cli sync-sales-intel-current` after your BigQuery refresh jobs complete."
        />
      ) : (
        <div className="data-table">
          <table>
            <thead>
              <tr>
                <th>Company</th>
                <th>Region</th>
                <th>Industry</th>
                <th>Priority</th>
                <th>Action</th>
                <th>Overall Score</th>
                <th>Matched Sources</th>
                <th>Intersection</th>
                <th>Reason To Contact</th>
              </tr>
            </thead>
            <tbody>
              {signals.map((signal) => (
                <tr key={signal.id}>
                  <td>
                    <strong>{signal.companyName}</strong>
                    <div className="subtle">
                      {[signal.siteCity, signal.siteState, signal.siteZip]
                        .filter(Boolean)
                        .join(", ")}
                    </div>
                  </td>
                  <td>{signal.region || "Unknown"}</td>
                  <td>{signal.industrySegment || "Unknown"}</td>
                  <td>{signal.currentPriority || "Unassigned"}</td>
                  <td>{signal.currentAction || "Monitor"}</td>
                  <td>{Math.round(signal.overallSalesScore)}</td>
                  <td>{signal.matchedSources || "OSHA only"}</td>
                  <td>
                    {intersectionCompanies.has(signal.companyNameNormalized) ? (
                      <span className="pill pill-alert">Alert</span>
                    ) : (
                      <span className="pill">None</span>
                    )}
                  </td>
                  <td>{signal.reasonToContact || signal.whyNow || "No reason exported"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </DashboardShell>
  );
}
