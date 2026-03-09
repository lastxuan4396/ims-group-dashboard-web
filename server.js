const http = require("http");
const path = require("path");
const fs = require("fs/promises");
const fssync = require("fs");
const { URL } = require("url");

const PORT = Number(process.env.PORT || 10000);
const DATA_DIR = path.join(__dirname, "data");
const DATA_FILE = path.join(DATA_DIR, "shared-state.json");
const MAX_HISTORY = 300;
const MAX_UNDO = 60;

let historyCounter = 0;
let persistTimer = null;
const sseClients = new Set();

let sharedState = {
  tasks: {},
  members: [],
  history: [],
  undoStack: [],
  updatedAt: null
};

function clone(data) {
  return JSON.parse(JSON.stringify(data));
}

function sanitizeName(name) {
  if (typeof name !== "string") return "";
  return name.trim().replace(/\s+/g, " ").slice(0, 40);
}

function sanitizeStatus(status) {
  return ["todo", "doing", "done"].includes(status) ? status : "todo";
}

function sanitizePriority(priority) {
  return ["low", "medium", "high"].includes(priority) ? priority : "medium";
}

function sanitizeDueDate(value) {
  if (typeof value !== "string") return "";
  const clean = value.trim();
  if (!clean) return "";
  if (!/^\d{4}-\d{2}-\d{2}$/.test(clean)) return "";
  const d = new Date(`${clean}T00:00:00`);
  if (Number.isNaN(d.getTime())) return "";
  return clean;
}

function defaultTask() {
  return {
    status: "todo",
    priority: "medium",
    dueDate: "",
    assignee: ""
  };
}

function normalizeTask(input) {
  if (!input || typeof input !== "object") return defaultTask();
  return {
    status: sanitizeStatus(input.status),
    priority: sanitizePriority(input.priority),
    dueDate: sanitizeDueDate(input.dueDate),
    assignee: sanitizeName(input.assignee || "")
  };
}

function normalizeHistoryEntry(entry) {
  if (!entry || typeof entry !== "object") return null;
  const at = typeof entry.at === "string" ? entry.at : new Date().toISOString();
  const actor = sanitizeName(entry.actor || "匿名") || "匿名";
  const kind = typeof entry.kind === "string" ? entry.kind : "task_update";
  const taskId = typeof entry.taskId === "string" ? entry.taskId : "";
  const message = typeof entry.message === "string" ? entry.message : "";
  const changes = entry.changes && typeof entry.changes === "object" ? entry.changes : {};
  const id = typeof entry.id === "string" ? entry.id : `${Date.now()}-${historyCounter += 1}`;
  return { id, at, actor, kind, taskId, message, changes };
}

function normalizeSnapshot(snapshot) {
  if (!snapshot || typeof snapshot !== "object") return null;
  const tasks = {};
  if (snapshot.tasks && typeof snapshot.tasks === "object") {
    Object.entries(snapshot.tasks).forEach(([taskId, task]) => {
      tasks[String(taskId)] = normalizeTask(task);
    });
  }

  const names = new Set();
  if (Array.isArray(snapshot.members)) {
    snapshot.members.forEach((member) => {
      const clean = sanitizeName(member);
      if (clean) names.add(clean);
    });
  }
  Object.values(tasks).forEach((task) => {
    if (task.assignee) names.add(task.assignee);
  });

  return {
    tasks,
    members: Array.from(names).sort((a, b) => a.localeCompare(b, "zh-CN"))
  };
}

function normalizeState(raw) {
  const normalized = {
    tasks: {},
    members: [],
    history: [],
    undoStack: [],
    updatedAt: typeof raw?.updatedAt === "string" ? raw.updatedAt : null
  };

  if (!raw || typeof raw !== "object") return normalized;

  if (raw.tasks && typeof raw.tasks === "object") {
    Object.entries(raw.tasks).forEach(([taskId, task]) => {
      normalized.tasks[String(taskId)] = normalizeTask(task);
    });
  } else {
    const checked = raw.checked && typeof raw.checked === "object" ? raw.checked : {};
    const assignees = raw.assignees && typeof raw.assignees === "object" ? raw.assignees : {};
    const ids = new Set([...Object.keys(checked), ...Object.keys(assignees)]);
    ids.forEach((taskId) => {
      const task = defaultTask();
      task.status = checked[taskId] ? "done" : "todo";
      task.assignee = sanitizeName(assignees[taskId] || "");
      normalized.tasks[taskId] = task;
    });
  }

  const members = new Set();
  if (Array.isArray(raw.members)) {
    raw.members.forEach((member) => {
      const clean = sanitizeName(member);
      if (clean) members.add(clean);
    });
  }
  Object.values(normalized.tasks).forEach((task) => {
    if (task.assignee) members.add(task.assignee);
  });
  normalized.members = Array.from(members).sort((a, b) => a.localeCompare(b, "zh-CN"));

  if (Array.isArray(raw.history)) {
    normalized.history = raw.history
      .map(normalizeHistoryEntry)
      .filter(Boolean)
      .slice(0, MAX_HISTORY);
  }

  if (Array.isArray(raw.undoStack)) {
    normalized.undoStack = raw.undoStack
      .map(normalizeSnapshot)
      .filter(Boolean)
      .slice(-MAX_UNDO);
  }

  return normalized;
}

function publicState() {
  return {
    tasks: sharedState.tasks,
    members: sharedState.members,
    history: sharedState.history,
    updatedAt: sharedState.updatedAt,
    undoDepth: sharedState.undoStack.length
  };
}

function pushHistory(entry) {
  const normalized = normalizeHistoryEntry(entry);
  if (!normalized) return;
  sharedState.history.unshift(normalized);
  if (sharedState.history.length > MAX_HISTORY) {
    sharedState.history = sharedState.history.slice(0, MAX_HISTORY);
  }
}

function pushUndoSnapshot() {
  sharedState.undoStack.push({
    tasks: clone(sharedState.tasks),
    members: clone(sharedState.members)
  });
  if (sharedState.undoStack.length > MAX_UNDO) {
    sharedState.undoStack.shift();
  }
}

function touchUpdatedAt() {
  sharedState.updatedAt = new Date().toISOString();
}

function ensureTask(taskId) {
  if (!sharedState.tasks[taskId]) {
    sharedState.tasks[taskId] = defaultTask();
  } else {
    sharedState.tasks[taskId] = normalizeTask(sharedState.tasks[taskId]);
  }
  return sharedState.tasks[taskId];
}

function applyTaskUpdate(taskId, changes, actor) {
  const task = ensureTask(taskId);
  const next = { ...task };

  if (Object.prototype.hasOwnProperty.call(changes, "status")) {
    next.status = sanitizeStatus(changes.status);
  }
  if (Object.prototype.hasOwnProperty.call(changes, "priority")) {
    next.priority = sanitizePriority(changes.priority);
  }
  if (Object.prototype.hasOwnProperty.call(changes, "dueDate")) {
    next.dueDate = sanitizeDueDate(changes.dueDate);
  }
  if (Object.prototype.hasOwnProperty.call(changes, "assignee")) {
    const assignee = sanitizeName(changes.assignee || "");
    if (assignee && !sharedState.members.includes(assignee)) {
      return { changed: false, error: "assignee_not_in_members" };
    }
    next.assignee = assignee;
  }

  const changedFields = {};
  ["status", "priority", "dueDate", "assignee"].forEach((field) => {
    if (task[field] !== next[field]) {
      changedFields[field] = {
        from: task[field],
        to: next[field]
      };
    }
  });

  if (Object.keys(changedFields).length === 0) {
    return { changed: false };
  }

  sharedState.tasks[taskId] = next;
  touchUpdatedAt();
  pushHistory({
    kind: "task_update",
    actor,
    taskId,
    changes: changedFields,
    message: ""
  });

  return { changed: true };
}

function addMember(member, actor) {
  const clean = sanitizeName(member);
  if (!clean || sharedState.members.includes(clean)) {
    return { changed: false };
  }

  sharedState.members.push(clean);
  sharedState.members.sort((a, b) => a.localeCompare(b, "zh-CN"));
  touchUpdatedAt();
  pushHistory({
    kind: "member_add",
    actor,
    message: clean,
    changes: {}
  });

  return { changed: true };
}

function removeMember(member, actor) {
  const clean = sanitizeName(member);
  if (!clean || !sharedState.members.includes(clean)) {
    return { changed: false };
  }

  sharedState.members = sharedState.members.filter((name) => name !== clean);
  const impacted = [];
  Object.entries(sharedState.tasks).forEach(([taskId, task]) => {
    if (task.assignee === clean) {
      task.assignee = "";
      impacted.push(taskId);
    }
  });

  touchUpdatedAt();
  pushHistory({
    kind: "member_remove",
    actor,
    message: impacted.length ? `${clean}（影响任务:${impacted.length}）` : clean,
    changes: {}
  });

  return { changed: true };
}

function replaceFromClient(incoming, actor) {
  const normalized = normalizeState(incoming);
  const sameTasks = JSON.stringify(normalized.tasks) === JSON.stringify(sharedState.tasks);
  const sameMembers = JSON.stringify(normalized.members) === JSON.stringify(sharedState.members);
  if (sameTasks && sameMembers) {
    return { changed: false };
  }

  sharedState.tasks = normalized.tasks;
  sharedState.members = normalized.members;
  touchUpdatedAt();
  pushHistory({
    kind: "replace",
    actor,
    message: "批量替换状态",
    changes: {}
  });

  return { changed: true };
}

function undoLast(actor) {
  if (!sharedState.undoStack.length) {
    return { changed: false, error: "undo_empty" };
  }

  const snapshot = sharedState.undoStack.pop();
  const normalized = normalizeSnapshot(snapshot);
  if (!normalized) {
    return { changed: false, error: "undo_invalid" };
  }

  sharedState.tasks = normalized.tasks;
  sharedState.members = normalized.members;
  touchUpdatedAt();
  pushHistory({
    kind: "undo",
    actor,
    message: "恢复上一操作",
    changes: {}
  });

  return { changed: true };
}

function sanitizePatchOperation(input) {
  if (!input || typeof input !== "object") return null;
  const actor = sanitizeName(input.actor || "") || "匿名";

  if (input.type === "task_update") {
    const taskId = typeof input.taskId === "string" ? input.taskId.trim().slice(0, 80) : "";
    if (!taskId) return null;

    const raw = input.changes && typeof input.changes === "object" ? input.changes : {};
    const changes = {};
    if (Object.prototype.hasOwnProperty.call(raw, "status")) {
      changes.status = sanitizeStatus(raw.status);
    }
    if (Object.prototype.hasOwnProperty.call(raw, "priority")) {
      changes.priority = sanitizePriority(raw.priority);
    }
    if (Object.prototype.hasOwnProperty.call(raw, "dueDate")) {
      changes.dueDate = sanitizeDueDate(raw.dueDate);
    }
    if (Object.prototype.hasOwnProperty.call(raw, "assignee")) {
      changes.assignee = sanitizeName(raw.assignee || "");
    }

    if (Object.keys(changes).length === 0) return null;
    return { type: "task_update", taskId, changes, actor };
  }

  if (input.type === "member_add") {
    return { type: "member_add", member: sanitizeName(input.member || ""), actor };
  }

  if (input.type === "member_remove") {
    return { type: "member_remove", member: sanitizeName(input.member || ""), actor };
  }

  if (typeof input.taskId === "string" && (
    Object.prototype.hasOwnProperty.call(input, "checked") ||
    Object.prototype.hasOwnProperty.call(input, "assignee")
  )) {
    const changes = {};
    if (Object.prototype.hasOwnProperty.call(input, "checked")) {
      changes.status = input.checked ? "done" : "todo";
    }
    if (Object.prototype.hasOwnProperty.call(input, "assignee")) {
      changes.assignee = sanitizeName(input.assignee || "");
    }
    return {
      type: "task_update",
      taskId: input.taskId.trim().slice(0, 80),
      changes,
      actor
    };
  }

  return null;
}

function applyOperation(operation) {
  if (!operation || typeof operation !== "object") {
    return { changed: false, error: "invalid_operation" };
  }

  if (operation.type === "undo") {
    return undoLast(operation.actor || "匿名");
  }

  pushUndoSnapshot();
  let result = { changed: false };

  if (operation.type === "task_update") {
    result = applyTaskUpdate(operation.taskId, operation.changes, operation.actor || "匿名");
  } else if (operation.type === "member_add") {
    result = addMember(operation.member, operation.actor || "匿名");
  } else if (operation.type === "member_remove") {
    result = removeMember(operation.member, operation.actor || "匿名");
  } else if (operation.type === "replace") {
    result = replaceFromClient(operation.state, operation.actor || "匿名");
  }

  if (!result.changed) {
    sharedState.undoStack.pop();
  }

  return result;
}

async function ensureFileStore() {
  await fs.mkdir(DATA_DIR, { recursive: true });
}

async function loadFromFile() {
  try {
    await ensureFileStore();
    const raw = await fs.readFile(DATA_FILE, "utf8");
    sharedState = normalizeState(JSON.parse(raw));
    console.log("Loaded state from file store.");
  } catch {
    console.log("No file store found, using empty state.");
  }
}

async function persistStateNow() {
  await ensureFileStore();
  await fs.writeFile(DATA_FILE, JSON.stringify(sharedState, null, 2), "utf8");
}

function schedulePersist() {
  clearTimeout(persistTimer);
  persistTimer = setTimeout(() => {
    persistStateNow().catch((error) => {
      console.error("Persist failed:", error.message);
    });
  }, 220);
}

function sendJson(res, status, data) {
  const text = JSON.stringify(data);
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": Buffer.byteLength(text)
  });
  res.end(text);
}

function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let raw = "";
    req.on("data", (chunk) => {
      raw += chunk;
      if (raw.length > 1_000_000) {
        reject(new Error("payload_too_large"));
        req.destroy();
      }
    });
    req.on("end", () => {
      if (!raw.trim()) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(raw));
      } catch {
        reject(new Error("invalid_json"));
      }
    });
    req.on("error", reject);
  });
}

function contentTypeByExt(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === ".html") return "text/html; charset=utf-8";
  if (ext === ".js") return "application/javascript; charset=utf-8";
  if (ext === ".css") return "text/css; charset=utf-8";
  if (ext === ".json") return "application/json; charset=utf-8";
  if (ext === ".png") return "image/png";
  if (ext === ".jpg" || ext === ".jpeg") return "image/jpeg";
  if (ext === ".svg") return "image/svg+xml";
  return "application/octet-stream";
}

function broadcastState() {
  const payload = `data: ${JSON.stringify({ state: publicState() })}\n\n`;
  sseClients.forEach((res) => {
    try {
      res.write(payload);
    } catch {
      sseClients.delete(res);
    }
  });
}

setInterval(() => {
  sseClients.forEach((res) => {
    try {
      res.write(": ping\n\n");
    } catch {
      sseClients.delete(res);
    }
  });
}, 25000);

const server = http.createServer(async (req, res) => {
  const method = req.method || "GET";
  const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);

  if (method === "GET" && url.pathname === "/healthz") {
    sendJson(res, 200, { status: "ok" });
    return;
  }

  if (method === "GET" && url.pathname === "/api/state") {
    sendJson(res, 200, { state: publicState() });
    return;
  }

  if (method === "GET" && url.pathname === "/events") {
    res.writeHead(200, {
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive"
    });
    sseClients.add(res);
    res.write(`data: ${JSON.stringify({ state: publicState() })}\n\n`);

    req.on("close", () => {
      sseClients.delete(res);
    });
    return;
  }

  if (method === "POST" && (url.pathname === "/api/patch" || url.pathname === "/api/replace" || url.pathname === "/api/undo")) {
    try {
      const payload = await parseJsonBody(req);
      let result = null;

      if (url.pathname === "/api/patch") {
        const operation = sanitizePatchOperation(payload);
        if (!operation) {
          sendJson(res, 400, { error: "invalid_patch" });
          return;
        }
        result = applyOperation(operation);
      } else if (url.pathname === "/api/replace") {
        const actor = sanitizeName(payload.actor || "") || "匿名";
        result = applyOperation({ type: "replace", state: payload, actor });
      } else {
        const actor = sanitizeName(payload.actor || "") || "匿名";
        result = applyOperation({ type: "undo", actor });
      }

      if (!result.changed) {
        sendJson(res, 200, { state: publicState(), changed: false, error: result.error || null });
        return;
      }

      schedulePersist();
      broadcastState();
      sendJson(res, 200, { state: publicState(), changed: true });
      return;
    } catch (error) {
      sendJson(res, 400, { error: error.message || "bad_request" });
      return;
    }
  }

  if (method === "GET") {
    const safePath = url.pathname === "/" ? "/index.html" : url.pathname;
    const normalizedPath = path.normalize(safePath).replace(/^\/+/, "");
    const absolute = path.join(__dirname, normalizedPath);

    if (!absolute.startsWith(__dirname)) {
      sendJson(res, 403, { error: "forbidden" });
      return;
    }

    if (fssync.existsSync(absolute) && fssync.statSync(absolute).isFile()) {
      try {
        const data = await fs.readFile(absolute);
        res.writeHead(200, {
          "Content-Type": contentTypeByExt(absolute),
          "Content-Length": data.length
        });
        res.end(data);
        return;
      } catch {
        sendJson(res, 500, { error: "file_read_failed" });
        return;
      }
    }

    sendJson(res, 404, { error: "not_found" });
    return;
  }

  sendJson(res, 405, { error: "method_not_allowed" });
});

async function bootstrap() {
  await loadFromFile();
  server.listen(PORT, () => {
    console.log(`IMS dashboard running at http://localhost:${PORT}`);
  });
}

bootstrap().catch((error) => {
  console.error("Bootstrap failed:", error);
  process.exit(1);
});

process.on("SIGTERM", () => {
  persistStateNow().finally(() => process.exit(0));
});
