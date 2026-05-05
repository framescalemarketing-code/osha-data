// Single Express serverless function for Vercel.
// All /api/* routes are rewritten here via vercel.json.
// Uses @google-cloud/bigquery instead of the bq CLI (unavailable on Vercel).
// In-memory state (cache, outcomes) survives warm instances; resets on cold start.

import express from "express";
import { fetchLeadsCached } from "./_lib/bq.mjs";

const app = express();
app.use(express.json());

// In-memory outcomes store (ephemeral, shared within one warm instance)
const outcomes = {};

// Pipeline trigger endpoints are not available in hosted mode —
// run `python -m pipeline.cli refresh-leads-v3` locally to refresh data.
const PIPELINE_UNAVAILABLE = {
  ok: false,
  error: "Pipeline triggers are not available in hosted mode. Run the pipeline locally.",
};

// ── Routes ────────────────────────────────────────────────────────────────────

app.get("/api/leads", async (req, res) => {
  try {
    const force = String(req.query?.force || "").trim() === "1";
    const payload = await fetchLeadsCached({ force });
    const mergedLeads = payload.leads.map((lead) => {
      const outcome = outcomes[lead.id] || {};
      return {
        ...lead,
        accountStatus: outcome.accountStatus || lead.accountStatus,
        outreachStatus: outcome.outreachStatus || "new",
        outreachNotes: outcome.outreachNotes || "",
        outreachUpdatedAt: outcome.outreachUpdatedAt || "",
      };
    });
    res.json({
      ok: true,
      count: mergedLeads.length,
      totalAvailable: payload.totalAvailable,
      generatedAt: payload.generatedAt,
      cacheHit: payload.cacheHit,
      stale: false,
      leads: mergedLeads,
    });
  } catch (error) {
    res.status(500).json({ ok: false, error: error instanceof Error ? error.message : "Failed to load leads" });
  }
});

app.get("/api/lead-outcomes", (_req, res) => {
  res.json({ ok: true, outcomes });
});

app.post("/api/lead-outcomes", (req, res) => {
  const leadId = String(req.body?.leadId || "").trim();
  const outreachStatus = String(req.body?.outreachStatus || "").trim();
  const outreachNotes = String(req.body?.outreachNotes || "").trim().slice(0, 2000);

  const allowedStatuses = new Set(["new", "attempted", "connected", "won", "lost"]);
  if (!leadId) { res.status(400).json({ ok: false, error: "leadId is required." }); return; }
  if (!allowedStatuses.has(outreachStatus)) { res.status(400).json({ ok: false, error: "Invalid outreachStatus." }); return; }

  const accountStatus =
    outreachStatus === "won" ? "Contacted"
    : outreachStatus === "lost" || outreachStatus === "connected" ? "In Review"
    : outreachStatus === "attempted" ? "Contacted"
    : "New";

  outcomes[leadId] = { accountStatus, outreachStatus, outreachNotes, outreachUpdatedAt: new Date().toISOString() };
  res.json({ ok: true, leadId, outcome: outcomes[leadId] });
});

app.get("/api/pull-status", (_req, res) => {
  res.json({ ok: true, currentPull: null });
});

app.get("/api/pull-history", (_req, res) => {
  res.json({ ok: true, history: [] });
});

app.post("/api/refresh-leads", (_req, res) => {
  res.status(501).json(PIPELINE_UNAVAILABLE);
});

app.post("/api/full-pipeline", (_req, res) => {
  res.status(501).json(PIPELINE_UNAVAILABLE);
});

export default app;
