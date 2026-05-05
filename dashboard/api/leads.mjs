// All /api/* routes are handled by api/index.mjs (single Express function).
// This file is unused; vercel.json routes /api/:path* → /api/index.
export default (_req, res) => res.status(404).json({ ok: false, error: "Not found" });
