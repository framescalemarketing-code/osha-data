import express from "express";
import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { fetchLeadsCached as fetchHostedLeadsCached } from "../api/_lib/bq.mjs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const dashboardRoot = path.resolve(__dirname, "..");
const repoRoot = path.resolve(dashboardRoot, "..");
const runtimeDir = path.resolve(dashboardRoot, ".runtime");
const historyFile = path.resolve(runtimeDir, "pull-history.json");
const outcomesFile = path.resolve(runtimeDir, "lead-outcomes.json");
const leadsSnapshotFile = path.resolve(runtimeDir, "leads-cache.json");
const port = Number(process.env.DASHBOARD_API_PORT || 8787);
const LEADS_CACHE_TTL_MS = 45_000;
const app = express();
app.use(express.json());

let currentPull = null;
let runtimeEnvCache = null;
const leadsCacheByMode = {
  primary: { leads: [], generatedAt: null, totalAvailable: null, cacheUntil: 0 },
  secondary: { leads: [], generatedAt: null, totalAvailable: null, cacheUntil: 0 },
};

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
  const env = await loadRuntimeEnvMap();

  return {
    projectId: env.get("PROJECT_ID") || "cold-lead-pipeline-dashboard",
    dataset: env.get("BQ_DATASET") || "osha_raw",
  };
}

async function loadRuntimeEnvMap() {
  const localPath = path.resolve(repoRoot, ".env.local");
  const fallbackPath = path.resolve(repoRoot, ".env");
  const dotenvPath = localPath;
  try {
    const text = await fs.readFile(dotenvPath, "utf8");
    return parseDotEnv(text);
  } catch (_error) {
    try {
      const fallbackText = await fs.readFile(fallbackPath, "utf8");
      return parseDotEnv(fallbackText);
    } catch (_fallbackError) {
      return new Map();
    }
  }
}

function toRuntimeEnv(dotenvMap) {
  const env = { ...process.env };
  for (const [key, value] of dotenvMap.entries()) {
    if (!env[key] || String(env[key]).trim() === "") {
      env[key] = value;
    }
  }
  return env;
}

function isInlineJson(value) {
  const trimmed = String(value || "").trim();
  return trimmed.startsWith("{") && trimmed.endsWith("}");
}

function maybeDecodeBase64Json(value) {
  const trimmed = String(value || "").trim();
  if (!trimmed) {
    return null;
  }
  try {
    const decoded = Buffer.from(trimmed, "base64").toString("utf8").trim();
    if (decoded.startsWith("{") && decoded.endsWith("}")) {
      return decoded;
    }
    return null;
  } catch (_error) {
    return null;
  }
}

async function resolveServiceAccountPath(runtimeEnv) {
  await ensureRuntimeDir();

  const pathCandidates = [
    runtimeEnv.GOOGLE_APPLICATION_CREDENTIALS,
    runtimeEnv.BIGQUERY_SERVICE_ACCOUNT_KEY_PATH,
    runtimeEnv.BIGQUERY_SERVICE_ACCOUNT_KEY_FILE,
    path.resolve(dashboardRoot, "bq-service-account.json"),
    path.resolve(repoRoot, "bq-service-account.json"),
    path.resolve(runtimeDir, "gcp-service-account.json"),
  ];
  const legacyValue = runtimeEnv.BigQuery_Service_Account_Key;
  if (legacyValue && !isInlineJson(legacyValue) && !maybeDecodeBase64Json(legacyValue)) {
    pathCandidates.push(legacyValue);
  }

  for (const candidate of pathCandidates) {
    if (!candidate || String(candidate).trim() === "") {
      continue;
    }
    const resolved = path.isAbsolute(candidate)
      ? candidate
      : path.resolve(repoRoot, String(candidate));
    try {
      await fs.access(resolved);
      return resolved;
    } catch (_error) {
      // Continue to inline fallback candidates.
    }
  }

  const inlineCandidates = [
    runtimeEnv.BIGQUERY_SERVICE_ACCOUNT_KEY_JSON,
    runtimeEnv.GOOGLE_SERVICE_ACCOUNT_JSON,
    runtimeEnv.GCP_SERVICE_ACCOUNT_JSON,
    runtimeEnv.BigQuery_Service_Account_Key,
  ];

  for (const candidate of inlineCandidates) {
    if (!candidate || String(candidate).trim() === "") {
      continue;
    }

    const maybeJson = isInlineJson(candidate)
      ? String(candidate).trim()
      : maybeDecodeBase64Json(candidate);
    if (!maybeJson) {
      continue;
    }

    const keyPath = path.resolve(runtimeDir, "gcp-service-account.json");
    await fs.writeFile(keyPath, maybeJson, "utf8");
    return keyPath;
  }

  return null;
}

async function getRuntimeEnv() {
  if (runtimeEnvCache) {
    return runtimeEnvCache;
  }

  const dotenvMap = await loadRuntimeEnvMap();
  const runtimeEnv = toRuntimeEnv(dotenvMap);
  const serviceAccountPath = await resolveServiceAccountPath(runtimeEnv);
  if (serviceAccountPath && (!runtimeEnv.GOOGLE_APPLICATION_CREDENTIALS || runtimeEnv.GOOGLE_APPLICATION_CREDENTIALS.trim() === "")) {
    runtimeEnv.GOOGLE_APPLICATION_CREDENTIALS = serviceAccountPath;
  }

  runtimeEnvCache = runtimeEnv;
  return runtimeEnv;
}

async function resolvePythonCommand() {
  const venvPython = path.resolve(repoRoot, ".venv", "Scripts", "python.exe");
  try {
    await fs.access(venvPython);
    return venvPython;
  } catch (_error) {
    return "python";
  }
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

async function readLeadsSnapshot() {
  await ensureRuntimeDir();
  try {
    const payload = await fs.readFile(leadsSnapshotFile, "utf8");
    const parsed = JSON.parse(payload);
    if (!parsed || typeof parsed !== "object" || !Array.isArray(parsed.leads)) {
      return null;
    }
    return {
      leads: parsed.leads,
      generatedAt: typeof parsed.generatedAt === "string" ? parsed.generatedAt : new Date().toISOString(),
      totalAvailable: Number.isFinite(Number(parsed.totalAvailable)) ? Number(parsed.totalAvailable) : null,
    };
  } catch (_error) {
    return null;
  }
}

async function writeLeadsSnapshot(leads, generatedAt, totalAvailable = null) {
  await ensureRuntimeDir();
  await fs.writeFile(
    leadsSnapshotFile,
    JSON.stringify({ leads, generatedAt, totalAvailable }, null, 2),
    "utf8",
  );
}

// Lead-row transformation lives in api/_lib/transforms.mjs for a single source of truth.
// The local dashboard server consumes already-transformed records via hosted fetch helpers.

function runCommand(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    let settled = false;
    const child = spawn(command, args, {
      cwd: options.cwd || repoRoot,
      env: { ...process.env, ...(options.env || {}) },
      shell: options.shell === true,
      stdio: ["pipe", "pipe", "pipe"],
    });

    const stdout = [];
    const stderr = [];
    let timeoutHandle = null;

    const finishWithError = (message) => {
      if (settled) {
        return;
      }
      settled = true;
      if (timeoutHandle) {
        clearTimeout(timeoutHandle);
      }
      reject(new Error(message));
    };

    if (typeof options.timeoutMs === "number" && options.timeoutMs > 0) {
      timeoutHandle = setTimeout(() => {
        const out = stdout.join("").trim();
        const err = stderr.join("").trim();
        const tail = `${out}\n${err}`.trim().split(/\r?\n/).slice(-20).join("\n");
        child.kill("SIGTERM");
        finishWithError(
          `Command timed out after ${options.timeoutMs}ms: ${command} ${args.join(" ")}\n${tail}`,
        );
      }, options.timeoutMs);
    }

    child.stdout.on("data", (chunk) => stdout.push(String(chunk)));
    child.stderr.on("data", (chunk) => stderr.push(String(chunk)));

    if (typeof options.input === "string") {
      child.stdin.write(options.input);
    }
    child.stdin.end();

    child.on("error", (error) => {
      finishWithError(error instanceof Error ? error.message : String(error));
    });
    child.on("close", (code) => {
      if (settled) {
        return;
      }
      settled = true;
      if (timeoutHandle) {
        clearTimeout(timeoutHandle);
      }
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

async function ensureBigQueryAuth() {
  const cfg = await loadPipelineConfig();
  await syncEnvForBigQueryClient(cfg);
  try {
    await fetchHostedLeadsCached({ force: true, includeSecondary: false });
    console.log("[dashboard-api] BigQuery client preflight passed.");
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    console.warn("[dashboard-api] BigQuery client preflight failed:", detail);
  }
}

function getMode(includeSecondary) {
  return includeSecondary ? "secondary" : "primary";
}

async function fetchLiveLeads({ includeSecondary = false } = {}) {
  const cfg = await loadPipelineConfig();
  await syncEnvForBigQueryClient(cfg);
  const payload = await fetchHostedLeadsCached({ force: true, includeSecondary });
  return Array.isArray(payload?.leads) ? payload.leads : [];
}

async function fetchLiveLeadTableCount() {
  const cfg = await loadPipelineConfig();
  await syncEnvForBigQueryClient(cfg);
  const payload = await fetchHostedLeadsCached({ force: false, includeSecondary: false });
  const total = Number(payload?.totalAvailable ?? 0);
  return Number.isFinite(total) ? total : 0;
}

async function syncEnvForBigQueryClient(cfg) {
  const runtimeEnv = await getRuntimeEnv();
  for (const [key, value] of Object.entries(runtimeEnv)) {
    if (process.env[key] == null || process.env[key] === "") {
      process.env[key] = value;
    }
  }
  process.env.PROJECT_ID = cfg.projectId;
  process.env.BQ_DATASET = cfg.dataset;
  process.env.ENABLE_LEAD_TABLE_COUNT = "1";
}

async function fetchLiveLeadsCached({ force = false, includeSecondary = false } = {}) {
  const mode = getMode(includeSecondary);
  const cache = leadsCacheByMode[mode];
  const now = Date.now();
  if (!force && cache.generatedAt && now < cache.cacheUntil) {
    return {
      leads: cache.leads,
      generatedAt: cache.generatedAt,
      totalAvailable: cache.totalAvailable,
      cacheHit: true,
    };
  }

  try {
    const [leads, totalAvailable] = await Promise.all([
      fetchLiveLeads({ includeSecondary }),
      fetchLiveLeadTableCount().catch(() => null),
    ]);
    const generatedAt = new Date().toISOString();
    leadsCacheByMode[mode] = {
      leads,
      generatedAt,
      totalAvailable,
      cacheUntil: Date.now() + LEADS_CACHE_TTL_MS,
    };
    if (!includeSecondary) {
      await writeLeadsSnapshot(leads, generatedAt, totalAvailable);
    }
    return {
      leads,
      generatedAt,
      totalAvailable,
      cacheHit: false,
      stale: false,
    };
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    console.warn("[dashboard-api] live lead fetch failed; using stale cache:", detail);
    if (cache.leads.length > 0 && cache.generatedAt) {
      return {
        leads: cache.leads,
        generatedAt: cache.generatedAt,
        totalAvailable: cache.totalAvailable,
        cacheHit: true,
        stale: true,
      };
    }
    throw error;
  }
}

function warmLeadsCacheInBackground({ includeSecondary = false } = {}) {
  fetchLiveLeadsCached({ force: true, includeSecondary }).catch((error) => {
    const detail = error instanceof Error ? error.message : String(error);
    console.warn("[dashboard-api] background lead cache warmup failed:", detail);
  });
}

async function appendHistory(entry) {
  const current = await readHistory();
  current.unshift(entry);
  const sliced = current.slice(0, 40);
  await writeHistory(sliced);
}

app.get("/api/leads", async (req, res) => {
  try {
    const force = String(req.query.force || "").trim() === "1";
    const includeSecondary = String(req.query.includeSecondary || "").trim() === "1";
    const leadPayload = await fetchLiveLeadsCached({ force, includeSecondary });
    const outcomes = await readOutcomes();
    const mergedLeads = leadPayload.leads.map((lead) => {
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
      totalAvailable: leadPayload.totalAvailable,
      generatedAt: leadPayload.generatedAt,
      cacheHit: leadPayload.cacheHit,
      includeSecondary,
      stale: !!leadPayload.stale,
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

// Bad leads endpoint â€” logged to a local JSON file for durable storage
const badLeadsFile = path.resolve(runtimeDir, "bad-leads.json");

async function readBadLeads() {
  try {
    const data = await fs.readFile(badLeadsFile, "utf8");
    return JSON.parse(data);
  } catch {
    return {};
  }
}

async function writeBadLeads(map) {
  await fs.writeFile(badLeadsFile, JSON.stringify(map, null, 2), "utf8");
}

app.get("/api/bad-leads", async (_req, res) => {
  const map = await readBadLeads();
  res.json({ ok: true, count: Object.keys(map).length, badLeads: Object.values(map) });
});

app.post("/api/bad-leads", async (req, res) => {
  const { leadId, company, industry, city, region, naicsCode, leadTier, reason } = req.body || {};
  const allowedReasons = new Set(["wrong_industry", "consumer_business", "out_of_business", "too_small", "duplicate", "other"]);
  if (!leadId || typeof leadId !== "string") {
    res.status(400).json({ ok: false, error: "leadId is required." });
    return;
  }
  if (reason && !allowedReasons.has(reason)) {
    res.status(400).json({ ok: false, error: "Invalid reason." });
    return;
  }
  const map = await readBadLeads();
  map[String(leadId).trim()] = {
    leadId: String(leadId).trim(),
    company: String(company || "").trim().slice(0, 200),
    industry: String(industry || "").trim().slice(0, 100),
    city: String(city || "").trim().slice(0, 100),
    region: String(region || "").trim().slice(0, 100),
    naicsCode: String(naicsCode || "").trim().slice(0, 20),
    leadTier: String(leadTier || "").trim().slice(0, 50),
    reason: String(reason || "other").trim(),
    markedAt: new Date().toISOString(),
  };
  await writeBadLeads(map);
  res.json({ ok: true });
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
    mode: "refresh",
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
    const runtimeEnv = await getRuntimeEnv();
    const pythonCommand = await resolvePythonCommand();
    await runCommand(pythonCommand, ["-m", "pipeline.cli", "refresh-leads-v3"], {
      cwd: repoRoot,
      env: runtimeEnv,
      timeoutMs: 180000,
    });

    const endedAt = new Date().toISOString();
    const durationSeconds = Math.round((Date.now() - startMs) / 1000);
    currentPull = {
      id: pullId,
      mode: "refresh",
      status: "success",
      startedAt,
      endedAt,
      durationSeconds,
    };

    warmLeadsCacheInBackground();

    await appendHistory({
      id: pullId,
      status: "success",
      startedAt,
      endedAt,
      durationSeconds,
      message: "Sales priority refresh completed.",
    });
  } catch (error) {
    const endedAt = new Date().toISOString();
    const durationSeconds = Math.round((Date.now() - startMs) / 1000);
    const detail = error instanceof Error ? error.message : "Pipeline pull failed";

    currentPull = {
      id: pullId,
      mode: "refresh",
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

app.post("/api/full-pipeline", async (_req, res) => {
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
    mode: "full",
    status: "running",
    startedAt,
  };

  appendHistory({
    id: pullId,
    status: "running",
    startedAt,
    endedAt: null,
    durationSeconds: null,
    message: "Full pipeline started.",
  }).catch(() => {});

  res.json({
    ok: true,
    pull: currentPull,
  });

  const startMs = Date.now();
  try {
    const runtimeEnv = await getRuntimeEnv();
    const pythonCommand = await resolvePythonCommand();
    await runCommand(pythonCommand, ["-m", "pipeline.cli", "run-full"], {
      cwd: repoRoot,
      env: runtimeEnv,
      timeoutMs: 900000,
    });

    const endedAt = new Date().toISOString();
    const durationSeconds = Math.round((Date.now() - startMs) / 1000);
    currentPull = {
      id: pullId,
      mode: "full",
      status: "success",
      startedAt,
      endedAt,
      durationSeconds,
    };

    warmLeadsCacheInBackground();

    await appendHistory({
      id: pullId,
      status: "success",
      startedAt,
      endedAt,
      durationSeconds,
      message: "Full pipeline run completed.",
    });
  } catch (error) {
    const endedAt = new Date().toISOString();
    const durationSeconds = Math.round((Date.now() - startMs) / 1000);
    const detail = error instanceof Error ? error.message : "Full pipeline failed";

    currentPull = {
      id: pullId,
      mode: "full",
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
  ensureBigQueryAuth().catch(() => {});
  readLeadsSnapshot()
    .then((snapshot) => {
      if (!snapshot) {
        return;
      }
      leadsCacheByMode.primary = {
        leads: snapshot.leads,
        generatedAt: snapshot.generatedAt,
        totalAvailable: snapshot.totalAvailable,
        cacheUntil: Date.now() + LEADS_CACHE_TTL_MS,
      };
    })
    .finally(() => {
      warmLeadsCacheInBackground();
    });
});

