import express from "express";
import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const dashboardRoot = path.resolve(__dirname, "..");
const repoRoot = path.resolve(dashboardRoot, "..");
const runtimeDir = path.resolve(dashboardRoot, ".runtime");
const historyFile = path.resolve(runtimeDir, "pull-history.json");
const outcomesFile = path.resolve(runtimeDir, "lead-outcomes.json");
const port = Number(process.env.DASHBOARD_API_PORT || 8787);

const app = express();
app.use(express.json());

let currentPull = null;

function parseDotEnv(rawText) {
  const map = new Map();
  for (const rawLine of rawText.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#") || !line.includes("=")) {
      continue;
    }
    const idx = line.indexOf("=");
    const key = line.slice(0, idx).trim();
    const value = line.slice(idx + 1).trim().replace(/^['"]|['"]$/g, "");
    map.set(key, value);
  }
  return map;
}

async function loadPipelineConfig() {
  const dotenvPath = path.resolve(repoRoot, ".env.local");
  const text = await fs.readFile(dotenvPath, "utf8");
  const env = parseDotEnv(text);

  return {
    projectId: env.get("PROJECT_ID") || "cold-lead-pipeline-dashboard",
    dataset: env.get("BQ_DATASET") || "osha_raw",
  };
}

async function ensureRuntimeDir() {
  await fs.mkdir(runtimeDir, { recursive: true });
}

async function readHistory() {
  await ensureRuntimeDir();
  try {
    const payload = await fs.readFile(historyFile, "utf8");
    const rows = JSON.parse(payload);
    return Array.isArray(rows) ? rows : [];
  } catch (error) {
    return [];
  }
}

async function writeHistory(history) {
  await ensureRuntimeDir();
  await fs.writeFile(historyFile, JSON.stringify(history, null, 2), "utf8");
}

async function readOutcomes() {
  await ensureRuntimeDir();
  try {
    const payload = await fs.readFile(outcomesFile, "utf8");
    const parsed = JSON.parse(payload);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch (_error) {
    return {};
  }
}

async function writeOutcomes(outcomes) {
  await ensureRuntimeDir();
  await fs.writeFile(outcomesFile, JSON.stringify(outcomes, null, 2), "utf8");
}

function normalizeCodes(rawStandards) {
  if (!rawStandards) {
    return [];
  }

  const tokens = String(rawStandards)
    .split("|")
    .map((item) => item.trim())
    .filter(Boolean);

  const codeRegex = /\b\d{4}\.\d+(?:\([^)]+\))*\b/g;
  const codes = [];
  for (const token of tokens) {
    const matches = token.match(codeRegex);
    if (matches) {
      for (const code of matches) {
        if (!codes.includes(code)) {
          codes.push(code);
        }
      }
    }
  }
  return codes;
}

function inferIncidentType(row) {
  const severe = String(row["Severe Incident Signal"] || "").toLowerCase() === "yes";
  const hasComplaint = String(row["Has Complaint Signal"] || "").toLowerCase() === "yes";
  const directPrescription = Number(row["Direct Prescription Citation Count"] || 0) > 0;
  const prescriptionSignal = Number(row["Prescription Signal Count"] || 0) > 0;
  const fitGap = Number(row["Fit Selection Citation Count"] || 0) > 0;
  const eyeFace = Number(row["Eye Face Citation Count"] || 0) > 0;
  const generalPpe = Number(row["General PPE Citation Count"] || 0) > 0;

  if (severe) return "Severe Injury";
  if (hasComplaint) return "Complaint Inspection";
  if (directPrescription || prescriptionSignal) return "Prescription Safety";
  if (fitGap) return "Fit And Training Gap";
  if (eyeFace && generalPpe) return "Chemical Exposure";
  if (eyeFace) return "Impact Hazard";
  return "General PPE";
}

function toLeadRecord(row) {
  const matchedSourcesRaw = String(row["Matched Sources"] || "").trim();
  const matchedSources = matchedSourcesRaw
    ? matchedSourcesRaw.split("|").map((item) => item.trim()).filter(Boolean)
    : ["OSHA"];

  const incidentDate =
    row["Last Accident Date"] ||
    row["Last Violation Event Date"] ||
    row["Latest Case Close Date"] ||
    row["Case Open Date"] ||
    null;

  const incidentDateIso = incidentDate ? String(incidentDate).slice(0, 10) : "";
  const now = Date.now();
  const lastTouchedDays = incidentDateIso
    ? Math.max(0, Math.floor((now - new Date(`${incidentDateIso}T00:00:00Z`).getTime()) / 86400000))
    : 0;

  return {
    id: `lead-${row["Latest Inspection ID"] || row["Account Name"] || Math.random().toString(16).slice(2)}`,
    company: row["Account Name"] || "Unknown Company",
    region: row["Region"] || "San Diego",
    city: row["Site City"] || "",
    industry: row["Industry Segment"] || "",
    ownerType: row["Ownership Type"] || "",
    overallSalesScore: Number(row["Overall Sales Score"] || 0),
    eyewearEvidenceScore: Number(row["Eyewear Evidence Score"] || row["OSHA Follow-up Score"] || 0),
    priority: row["Overall Sales Priority"] || "P3 Monitor",
    needTier: row["Eyewear Need Tier"] || "Fit Only",
    action: row["Should Look At Now"] || "Monitor / Nurture",
    matchedSources,
    reasonToContact: row["Reason To Contact"] || "",
    whyNow: row["Why Now"] || "",
    recentInspectionContext: row["Recent Inspection Context"] || "",
    incidentDate: incidentDateIso,
    incidentType: inferIncidentType(row),
    rawViolationCodes: normalizeCodes(row["Standards Cited"]),
    openViolations: String(row["Has Open Violations"] || "").toLowerCase() === "yes",
    severeIncident: String(row["Severe Incident Signal"] || "").toLowerCase() === "yes",
    employeeBand: row["Estimated Employee Band"] || "Unknown",
    lastTouchedDays,
    accountStatus: "New",
  };
}

function runCommand(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: options.cwd || repoRoot,
      env: { ...process.env, ...(options.env || {}) },
      shell: process.platform === "win32",
      stdio: ["ignore", "pipe", "pipe"],
    });

    const stdout = [];
    const stderr = [];

    child.stdout.on("data", (chunk) => stdout.push(String(chunk)));
    child.stderr.on("data", (chunk) => stderr.push(String(chunk)));

    child.on("error", reject);
    child.on("close", (code) => {
      const out = stdout.join("");
      const err = stderr.join("");
      if (code !== 0) {
        reject(new Error(err || out || `${command} exited with code ${code}`));
        return;
      }
      resolve(out);
    });
  });
}

async function fetchLiveLeads() {
  const cfg = await loadPipelineConfig();
  const sql = `
WITH actionable AS (
  SELECT *
  FROM \`${cfg.projectId}.${cfg.dataset}.eyewear_opportunity_actionable_current\`
),
followup AS (
  SELECT *
  FROM \`${cfg.projectId}.${cfg.dataset}.sales_followup_all_current\`
),
joined AS (
  SELECT
    a.*,
    f.\`Case Open Date\`,
    f.\`Latest Case Close Date\`,
    f.\`Last Violation Event Date\`,
    f.\`Last Accident Date\`,
    f.\`Has Complaint Signal\`,
    f.\`Standards Cited\`,
    f.\`Company Latest Load Timestamp\`
  FROM actionable a
  LEFT JOIN followup f
    ON a.\`Latest Inspection ID\` = f.\`Latest Inspection ID\`
   AND a.\`Region\` = f.\`Region\`
   AND a.\`Account Name\` = f.\`Account Name\`
)
SELECT * EXCEPT(rn)
FROM (
  SELECT
    joined.*,
    ROW_NUMBER() OVER (
      PARTITION BY \`Latest Inspection ID\`, \`Region\`, \`Account Name\`
      ORDER BY \`Company Latest Load Timestamp\` DESC NULLS LAST
    ) AS rn
  FROM joined
)
WHERE rn = 1
ORDER BY \`Overall Sales Score\` DESC
LIMIT 500
`;
  const sqlOneLine = sql.replace(/\s+/g, " ").trim();

  const rawJson = await runCommand("bq", [
    `--project_id=${cfg.projectId}`,
    "query",
    "--use_legacy_sql=false",
    "--format=prettyjson",
    sqlOneLine,
  ]);

  const rows = JSON.parse(rawJson);
  return rows.map(toLeadRecord);
}

async function appendHistory(entry) {
  const current = await readHistory();
  current.unshift(entry);
  const sliced = current.slice(0, 40);
  await writeHistory(sliced);
}

app.get("/api/leads", async (_req, res) => {
  try {
    const leads = await fetchLiveLeads();
    const outcomes = await readOutcomes();
    const mergedLeads = leads.map((lead) => {
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
      generatedAt: new Date().toISOString(),
      leads: mergedLeads,
    });
  } catch (error) {
    res.status(500).json({
      ok: false,
      error: error instanceof Error ? error.message : "Failed to load leads",
    });
  }
});

app.get("/api/lead-outcomes", async (_req, res) => {
  const outcomes = await readOutcomes();
  res.json({
    ok: true,
    outcomes,
  });
});

app.post("/api/lead-outcomes", async (req, res) => {
  const leadId = String(req.body?.leadId || "").trim();
  const outreachStatus = String(req.body?.outreachStatus || "").trim();
  const outreachNotes = String(req.body?.outreachNotes || "").trim().slice(0, 2000);

  const allowedStatuses = new Set(["new", "attempted", "connected", "won", "lost"]);
  if (!leadId) {
    res.status(400).json({ ok: false, error: "leadId is required." });
    return;
  }
  if (!allowedStatuses.has(outreachStatus)) {
    res.status(400).json({ ok: false, error: "Invalid outreachStatus." });
    return;
  }

  const outcomes = await readOutcomes();
  const accountStatus =
    outreachStatus === "won"
      ? "Contacted"
      : outreachStatus === "lost" || outreachStatus === "connected"
        ? "In Review"
        : outreachStatus === "attempted"
          ? "Contacted"
          : "New";

  outcomes[leadId] = {
    accountStatus,
    outreachStatus,
    outreachNotes,
    outreachUpdatedAt: new Date().toISOString(),
  };
  await writeOutcomes(outcomes);

  res.json({
    ok: true,
    leadId,
    outcome: outcomes[leadId],
  });
});

app.get("/api/pull-status", (_req, res) => {
  res.json({
    ok: true,
    currentPull,
  });
});

app.get("/api/pull-history", async (_req, res) => {
  const history = await readHistory();
  res.json({
    ok: true,
    history,
  });
});

app.post("/api/refresh-leads", async (_req, res) => {
  if (currentPull?.status === "running") {
    res.status(409).json({
      ok: false,
      error: "A pull is already running.",
      currentPull,
    });
    return;
  }

  const startedAt = new Date().toISOString();
  const pullId = `pull-${Date.now()}`;
  currentPull = {
    id: pullId,
    status: "running",
    startedAt,
  };

  appendHistory({
    id: pullId,
    status: "running",
    startedAt,
    endedAt: null,
    durationSeconds: null,
    message: "Pull started.",
  }).catch(() => {});

  res.json({
    ok: true,
    pull: currentPull,
  });

  const startMs = Date.now();
  try {
    await runCommand("python", ["-m", "pipeline.cli", "run-full"], {
      cwd: repoRoot,
    });

    const endedAt = new Date().toISOString();
    const durationSeconds = Math.round((Date.now() - startMs) / 1000);
    currentPull = {
      id: pullId,
      status: "success",
      startedAt,
      endedAt,
      durationSeconds,
    };

    await appendHistory({
      id: pullId,
      status: "success",
      startedAt,
      endedAt,
      durationSeconds,
      message: "Pipeline run-full completed.",
    });
  } catch (error) {
    const endedAt = new Date().toISOString();
    const durationSeconds = Math.round((Date.now() - startMs) / 1000);
    const detail = error instanceof Error ? error.message : "Pipeline pull failed";

    currentPull = {
      id: pullId,
      status: "failed",
      startedAt,
      endedAt,
      durationSeconds,
      error: detail,
    };

    await appendHistory({
      id: pullId,
      status: "failed",
      startedAt,
      endedAt,
      durationSeconds,
      message: detail,
    });
  }
});

app.listen(port, () => {
  console.log(`[dashboard-api] running on http://127.0.0.1:${port}`);
});
