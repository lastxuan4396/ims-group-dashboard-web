const express = require("express");
const http = require("http");
const path = require("path");
const fs = require("fs/promises");
const { Pool } = require("pg");
const { Server } = require("socket.io");

const PORT = process.env.PORT || 10000;
const STORE_ID = "main";
const DATA_DIR = path.join(__dirname, "data");
const DATA_FILE = path.join(DATA_DIR, "shared-state.json");

let sharedState = {
  checked: {},
  assignees: {},
  updatedAt: null
};

let persistTimer = null;
let pool = null;

function useSslForDb(url) {
  if (!url) return false;
  if (process.env.PGSSLMODE === "require") return { rejectUnauthorized: false };
  return url.includes("render.com:5432") ? { rejectUnauthorized: false } : false;
}

function sanitizeAssignee(name) {
  if (typeof name !== "string") return "";
  return name.trim().replace(/\s+/g, " ").slice(0, 40);
}

function normalizeState(input) {
  const normalized = {
    checked: {},
    assignees: {},
    updatedAt: input && typeof input.updatedAt === "string" ? input.updatedAt : null
  };

  if (input && typeof input === "object") {
    if (input.checked && typeof input.checked === "object") {
      Object.entries(input.checked).forEach(([key, value]) => {
        if (value === true) normalized.checked[String(key)] = true;
      });
    }
    if (input.assignees && typeof input.assignees === "object") {
      Object.entries(input.assignees).forEach(([key, value]) => {
        const cleaned = sanitizeAssignee(value);
        if (cleaned) normalized.assignees[String(key)] = cleaned;
      });
    }
  }

  return normalized;
}

function sanitizePatch(input) {
  if (!input || typeof input !== "object") return null;
  const rawId = typeof input.taskId === "string" ? input.taskId.trim() : "";
  if (!rawId) return null;

  const patch = { taskId: rawId.slice(0, 80) };
  if (Object.prototype.hasOwnProperty.call(input, "checked")) {
    patch.checked = Boolean(input.checked);
  }
  if (Object.prototype.hasOwnProperty.call(input, "assignee")) {
    const cleaned = sanitizeAssignee(input.assignee || "");
    patch.assignee = cleaned || null;
  }

  if (!Object.prototype.hasOwnProperty.call(patch, "checked") &&
      !Object.prototype.hasOwnProperty.call(patch, "assignee")) {
    return null;
  }
  return patch;
}

function applyPatch(patch) {
  if (Object.prototype.hasOwnProperty.call(patch, "checked")) {
    if (patch.checked) sharedState.checked[patch.taskId] = true;
    else delete sharedState.checked[patch.taskId];
  }

  if (Object.prototype.hasOwnProperty.call(patch, "assignee")) {
    if (patch.assignee) sharedState.assignees[patch.taskId] = patch.assignee;
    else delete sharedState.assignees[patch.taskId];
  }

  sharedState.updatedAt = new Date().toISOString();
}

function replaceState(nextState) {
  const normalized = normalizeState(nextState);
  sharedState = {
    checked: normalized.checked,
    assignees: normalized.assignees,
    updatedAt: new Date().toISOString()
  };
}

async function ensureFileStore() {
  await fs.mkdir(DATA_DIR, { recursive: true });
}

async function loadFromFile() {
  try {
    await ensureFileStore();
    const raw = await fs.readFile(DATA_FILE, "utf8");
    const parsed = JSON.parse(raw);
    sharedState = normalizeState(parsed);
    sharedState.updatedAt = parsed.updatedAt || null;
    console.log("Loaded state from file store.");
  } catch {
    console.log("No file store found, using empty state.");
  }
}

async function saveToFile() {
  await ensureFileStore();
  await fs.writeFile(DATA_FILE, JSON.stringify(sharedState, null, 2), "utf8");
}

async function initDbIfAvailable() {
  const dbUrl = process.env.DATABASE_URL;
  if (!dbUrl) {
    console.log("DATABASE_URL is not set. Using file store.");
    return;
  }

  try {
    pool = new Pool({
      connectionString: dbUrl,
      ssl: useSslForDb(dbUrl)
    });

    await pool.query(`
      CREATE TABLE IF NOT EXISTS ims_dashboard_state (
        id TEXT PRIMARY KEY,
        payload JSONB NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

    const result = await pool.query(
      "SELECT payload, updated_at FROM ims_dashboard_state WHERE id = $1",
      [STORE_ID]
    );

    if (result.rows.length > 0) {
      const row = result.rows[0];
      const normalized = normalizeState(row.payload);
      sharedState = {
        checked: normalized.checked,
        assignees: normalized.assignees,
        updatedAt: row.updated_at ? new Date(row.updated_at).toISOString() : null
      };
      console.log("Loaded state from Postgres.");
    } else {
      console.log("Postgres table ready but empty, using current state.");
    }
  } catch (error) {
    console.error("Postgres init failed, fallback to file store:", error.message);
    if (pool) {
      await pool.end().catch(() => {});
      pool = null;
    }
  }
}

async function persistStateNow() {
  if (pool) {
    await pool.query(
      `
        INSERT INTO ims_dashboard_state (id, payload, updated_at)
        VALUES ($1, $2::jsonb, NOW())
        ON CONFLICT (id)
        DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()
      `,
      [STORE_ID, JSON.stringify(sharedState)]
    );
    return;
  }
  await saveToFile();
}

function schedulePersist() {
  clearTimeout(persistTimer);
  persistTimer = setTimeout(() => {
    persistStateNow().catch((error) => {
      console.error("Persist failed:", error.message);
    });
  }, 220);
}

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*"
  }
});

app.get("/healthz", (_req, res) => {
  res.status(200).json({ status: "ok" });
});

app.get("/api/state", (_req, res) => {
  res.json({ state: sharedState });
});

app.use(express.static(__dirname));

app.get("*", (_req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

io.on("connection", (socket) => {
  socket.emit("state:init", { state: sharedState });

  socket.on("state:request", () => {
    socket.emit("state:init", { state: sharedState });
  });

  socket.on("state:patch", (incoming) => {
    const patch = sanitizePatch(incoming);
    if (!patch) {
      socket.emit("state:error", { message: "Invalid patch payload" });
      return;
    }

    applyPatch(patch);
    schedulePersist();
    io.emit("state:patch", {
      ...patch,
      updatedAt: sharedState.updatedAt
    });
  });

  socket.on("state:replace", (incoming) => {
    replaceState(incoming);
    schedulePersist();
    io.emit("state:replace", {
      state: sharedState,
      updatedAt: sharedState.updatedAt
    });
  });
});

async function bootstrap() {
  await loadFromFile();
  await initDbIfAvailable();

  server.listen(PORT, () => {
    console.log(`IMS dashboard running at http://localhost:${PORT}`);
  });
}

bootstrap().catch((error) => {
  console.error("Bootstrap failed:", error);
  process.exit(1);
});

process.on("SIGTERM", () => {
  persistStateNow().finally(() => {
    process.exit(0);
  });
});
