(() => {
  const UI_STORAGE_KEY = "ims-dashboard-ui-v2";
  const CACHE_STORAGE_KEY = "ims-dashboard-cache-v2";
  const ADD_MEMBER_VALUE = "__add_member__";

  const STATUS_LABELS = {
    todo: "未开始",
    doing: "进行中",
    done: "已完成"
  };

  const PRIORITY_LABELS = {
    low: "低",
    medium: "中",
    high: "高"
  };

  const steps = [
    {
      id: "s1",
      label: "Step 1",
      title: "Executive Summary",
      focus: "只写结论，不展开过程",
      tasks: [
        "Blanboz是谁、当前提供什么价值",
        "为什么现在值得进入欧洲 BESS 咨询市场",
        "为什么优先 Germany / Romania / Poland",
        "核心商业模式：咨询服务为主，平台为增强器",
        "未来 2-3 年增长路径概览"
      ]
    },
    {
      id: "s2",
      label: "Step 2",
      title: "1. The Opportunity",
      focus: "回答为什么现在值得做这门生意",
      tasks: [
        "1.1 Company overview",
        "1.2 The business problem",
        "1.3 Why now"
      ]
    },
    {
      id: "s3",
      label: "Step 3",
      title: "2. Market Analysis",
      focus: "讲清为什么是 Germany / Romania / Poland",
      tasks: [
        "2.1 Country screening and market selection",
        "2.2 Industry structure in the selected markets",
        "2.3 Portfolio rationale"
      ]
    },
    {
      id: "s4",
      label: "Step 4",
      title: "3. Marketing",
      focus: "谁会买、为什么买、怎么转成客户",
      tasks: [
        "3.1 Target customers",
        "3.2 Buyer roles and decision chain",
        "3.3 Buying triggers",
        "3.4 Buying journey",
        "3.5 Offer packaging"
      ]
    },
    {
      id: "s5",
      label: "Step 5",
      title: "4. Operational Plan",
      focus: "交付模式、能力配置、90天执行",
      tasks: [
        "4.1 Delivery model",
        "4.2 Capability model",
        "4.3 90-day operating plan",
        "4.4 Platform as an optional enabler"
      ]
    },
    {
      id: "s6",
      label: "Step 6",
      title: "5. International Strategy",
      focus: "三国并行与复制扩张逻辑",
      tasks: [
        "5.1 Why a portfolio approach",
        "5.2 Country roles in the portfolio",
        "5.3 Country-specific adaptation",
        "5.4 Expansion logic"
      ]
    },
    {
      id: "s7",
      label: "Step 7",
      title: "6. Financial Plan",
      focus: "收入逻辑、假设、盈亏与情景",
      tasks: [
        "6.1 Revenue model",
        "6.2 Pricing logic",
        "6.3 Key assumptions",
        "6.4 3-year revenue outlook",
        "6.5 Break-even and sensitivity",
        "6.6 Platform scenario note"
      ]
    },
    {
      id: "s8",
      label: "Step 8",
      title: "Conclusion & Next Steps",
      focus: "回到战略与执行层面收尾",
      tasks: [
        "最终战略结论：为什么 Germany / Romania / Poland 是合理优先组合",
        "当前最优先的 3-5 个动作（模板、试点、客户名单、合作伙伴）",
        "平台决策点：先模板化筛选，或试点验证后启动 MVP"
      ]
    }
  ];

  const sprintPhases = [
    {
      id: "p1",
      title: "Days 1-30 · Build",
      goal: "先建立可复用模板与目标名单",
      tasks: [
        "完成收益模型、合规清单、证据附件模板",
        "完成目标客户名单与优先联系清单",
        "完成合作伙伴短名单（并网/工程/法律等）"
      ]
    },
    {
      id: "p2",
      title: "Days 31-60 · Pilot",
      goal: "用 discovery 试点验证需求与转化",
      tasks: [
        "跑通 opportunity signal -> paid discovery 路径",
        "形成首批付费诊断项目",
        "沉淀线索质量、触发点与转化障碍"
      ]
    },
    {
      id: "p3",
      title: "Days 61-90 · Scale",
      goal: "升级高价值项目并评估平台选项",
      tasks: [
        "把 discovery 升级为 project / retainer",
        "建立稳定合作伙伴协作机制",
        "根据试点结果决定是否启动 MVP"
      ]
    }
  ];

  const riskItems = [
    "把平台写成业务前提：偏离“可选增强器”的核心逻辑",
    "国家组合被写成硬排序：丢失“三国同优先梯队”逻辑",
    "客户链条写得过泛：缺少真实 buyer roles 与触发点",
    "财务假设口径不统一：价格/转化/产能无法互相验证",
    "章节衔接断层：market、marketing、operations 无闭环"
  ];

  let eventSource = null;
  let syncConnected = false;
  const pendingMessages = [];
  const uiState = loadUiState();
  const sharedState = normalizeSharedState(loadSharedCache());

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

  function sanitizeDueDate(date) {
    if (typeof date !== "string") return "";
    const v = date.trim();
    if (!v) return "";
    if (!/^\d{4}-\d{2}-\d{2}$/.test(v)) return "";
    const d = new Date(`${v}T00:00:00`);
    if (Number.isNaN(d.getTime())) return "";
    return v;
  }

  function defaultTaskState() {
    return {
      status: "todo",
      priority: "medium",
      dueDate: "",
      assignee: ""
    };
  }

  function normalizeTask(input) {
    const base = defaultTaskState();
    if (!input || typeof input !== "object") return base;
    return {
      status: sanitizeStatus(input.status),
      priority: sanitizePriority(input.priority),
      dueDate: sanitizeDueDate(input.dueDate),
      assignee: sanitizeName(input.assignee || "")
    };
  }

  function normalizeHistoryEntry(entry) {
    if (!entry || typeof entry !== "object") return null;
    const id = typeof entry.id === "string" ? entry.id : `log-${Date.now()}`;
    const at = typeof entry.at === "string" ? entry.at : new Date().toISOString();
    const actor = sanitizeName(entry.actor || "匿名");
    const kind = typeof entry.kind === "string" ? entry.kind : "task_update";
    const taskId = typeof entry.taskId === "string" ? entry.taskId : "";
    const message = typeof entry.message === "string" ? entry.message : "";
    const changes = entry.changes && typeof entry.changes === "object" ? entry.changes : {};
    return { id, at, actor, kind, taskId, message, changes };
  }

  function normalizeSharedState(raw) {
    const state = {
      tasks: {},
      members: [],
      history: [],
      updatedAt: null,
      undoDepth: 0
    };

    if (!raw || typeof raw !== "object") return state;

    if (raw.tasks && typeof raw.tasks === "object") {
      Object.entries(raw.tasks).forEach(([taskId, task]) => {
        state.tasks[String(taskId)] = normalizeTask(task);
      });
    } else {
      const checked = raw.checked && typeof raw.checked === "object" ? raw.checked : {};
      const assignees = raw.assignees && typeof raw.assignees === "object" ? raw.assignees : {};
      const ids = new Set([...Object.keys(checked), ...Object.keys(assignees)]);
      ids.forEach((taskId) => {
        const base = defaultTaskState();
        base.status = checked[taskId] ? "done" : "todo";
        base.assignee = sanitizeName(assignees[taskId] || "");
        state.tasks[taskId] = base;
      });
    }

    const membersSet = new Set();
    if (Array.isArray(raw.members)) {
      raw.members.forEach((member) => {
        const name = sanitizeName(member);
        if (name) membersSet.add(name);
      });
    }
    Object.values(state.tasks).forEach((task) => {
      if (task.assignee) membersSet.add(task.assignee);
    });
    state.members = Array.from(membersSet).sort((a, b) => a.localeCompare(b, "zh-CN"));

    if (Array.isArray(raw.history)) {
      state.history = raw.history.map(normalizeHistoryEntry).filter(Boolean).slice(0, 300);
    }

    if (typeof raw.updatedAt === "string") state.updatedAt = raw.updatedAt;
    if (Number.isFinite(raw.undoDepth)) state.undoDepth = Math.max(0, Number(raw.undoDepth));

    return state;
  }

  function loadUiState() {
    try {
      const data = JSON.parse(localStorage.getItem(UI_STORAGE_KEY) || "{}");
      return {
        hideDone: Boolean(data.hideDone),
        dueWeekOnly: Boolean(data.dueWeekOnly),
        assigneeFilter: typeof data.assigneeFilter === "string" ? data.assigneeFilter : "all",
        currentMember: sanitizeName(data.currentMember || "")
      };
    } catch {
      return {
        hideDone: false,
        dueWeekOnly: false,
        assigneeFilter: "all",
        currentMember: ""
      };
    }
  }

  function saveUiState() {
    localStorage.setItem(UI_STORAGE_KEY, JSON.stringify(uiState));
  }

  function loadSharedCache() {
    try {
      return JSON.parse(localStorage.getItem(CACHE_STORAGE_KEY) || "{}");
    } catch {
      return {};
    }
  }

  function saveSharedCache() {
    const cached = {
      tasks: sharedState.tasks,
      members: sharedState.members,
      history: sharedState.history,
      updatedAt: sharedState.updatedAt,
      undoDepth: sharedState.undoDepth
    };
    localStorage.setItem(CACHE_STORAGE_KEY, JSON.stringify(cached));
  }

  function setSyncStatus(isOnline, text) {
    const box = document.getElementById("syncStatus");
    box.textContent = text;
    box.classList.toggle("offline", !isOnline);
  }

  function taskId(groupId, idx) {
    return `${groupId}-t${idx + 1}`;
  }

  function getTaskDefMap() {
    const map = {};
    steps.forEach((step) => {
      step.tasks.forEach((text, idx) => {
        map[taskId(step.id, idx)] = { text, zone: step.label };
      });
    });
    sprintPhases.forEach((phase) => {
      phase.tasks.forEach((text, idx) => {
        map[taskId(phase.id, idx)] = { text, zone: phase.title };
      });
    });
    riskItems.forEach((text, idx) => {
      map[taskId("risk", idx)] = { text, zone: "风险监控" };
    });
    return map;
  }

  const taskDefMap = getTaskDefMap();
  ensureKnownTasks();

  function getStepDisplayLabel(step) {
    const title = String(step?.title || "");
    const match = title.match(/^(\d+)\./);
    if (match) return `章 ${match[1]}`;
    if (/Executive Summary/i.test(title)) return "摘要";
    if (/Conclusion/i.test(title)) return "结论";
    return step?.label || "章节";
  }

  function ensureKnownTasks() {
    Object.keys(taskDefMap).forEach((id) => {
      if (!sharedState.tasks[id]) {
        sharedState.tasks[id] = defaultTaskState();
      } else {
        sharedState.tasks[id] = normalizeTask(sharedState.tasks[id]);
      }
    });
  }

  function getTask(id) {
    if (!sharedState.tasks[id]) sharedState.tasks[id] = defaultTaskState();
    return sharedState.tasks[id];
  }

  function createSelect(options, value) {
    const select = document.createElement("select");
    select.className = "field-input";
    options.forEach((opt) => {
      const o = document.createElement("option");
      o.value = opt.value;
      o.textContent = opt.label;
      select.appendChild(o);
    });
    select.value = value;
    return select;
  }

  function fillAssigneeSelect(select, selected) {
    const current = selected || "";
    const names = [...sharedState.members];
    if (current && !names.includes(current)) names.push(current);

    select.innerHTML = "";
    const blank = document.createElement("option");
    blank.value = "";
    blank.textContent = "负责人";
    select.appendChild(blank);

    names.sort((a, b) => a.localeCompare(b, "zh-CN")).forEach((name) => {
      const option = document.createElement("option");
      option.value = name;
      option.textContent = name;
      select.appendChild(option);
    });
    select.value = current;
  }

  function applyFieldClass(select, value, prefix) {
    select.classList.remove("todo", "doing", "done", "low", "medium", "high");
    select.classList.add(prefix === "status" ? "status-select" : "priority-select");
    select.classList.add(value);
  }

  function createTaskRow(id, text, stepId = "") {
    const row = document.createElement("div");
    row.className = "task";
    row.dataset.taskId = id;
    row.dataset.stepId = stepId;

    const main = document.createElement("div");
    main.className = "task-main";
    const indexMatch = String(text).match(/^(\d+(?:\.\d+)*)\s+(.+)$/);
    if (indexMatch) {
      const indexChip = document.createElement("span");
      indexChip.className = "task-index";
      indexChip.textContent = indexMatch[1];
      main.appendChild(indexChip);
      text = indexMatch[2];
    }
    const label = document.createElement("span");
    label.className = "task-label";
    label.textContent = text;
    main.appendChild(label);

    const statusSelect = createSelect([
      { value: "todo", label: "未开始" },
      { value: "doing", label: "进行中" },
      { value: "done", label: "已完成" }
    ], getTask(id).status);
    statusSelect.dataset.field = "status";
    statusSelect.addEventListener("change", () => {
      handleTaskFieldChange(id, { status: statusSelect.value }, stepId);
    });

    const prioritySelect = createSelect([
      { value: "high", label: "高" },
      { value: "medium", label: "中" },
      { value: "low", label: "低" }
    ], getTask(id).priority);
    prioritySelect.dataset.field = "priority";
    prioritySelect.addEventListener("change", () => {
      handleTaskFieldChange(id, { priority: prioritySelect.value });
    });

    const dueInput = document.createElement("input");
    dueInput.type = "date";
    dueInput.className = "field-input";
    dueInput.dataset.field = "dueDate";
    dueInput.value = getTask(id).dueDate;
    dueInput.addEventListener("change", () => {
      handleTaskFieldChange(id, { dueDate: dueInput.value });
    });

    const assigneeSelect = document.createElement("select");
    assigneeSelect.className = "field-input";
    assigneeSelect.dataset.field = "assignee";
    fillAssigneeSelect(assigneeSelect, getTask(id).assignee);
    assigneeSelect.addEventListener("change", () => {
      handleTaskFieldChange(id, { assignee: assigneeSelect.value });
    });

    row.appendChild(main);
    row.appendChild(statusSelect);
    row.appendChild(prioritySelect);
    row.appendChild(dueInput);
    row.appendChild(assigneeSelect);
    return row;
  }

  function render() {
    renderSteps();
    renderSprint();
    renderRisk();
    bindTopButtons();
    bindCollabControls();
    bindStarterActions();
    syncAllUiFromState();
    connectRealtime();
  }

  function renderSteps() {
    const container = document.getElementById("stepsContainer");
    container.innerHTML = "";

    steps.forEach((step) => {
      const card = document.createElement("article");
      card.className = "step-card";
      card.dataset.stepId = step.id;
      const displayLabel = getStepDisplayLabel(step);

      const title = document.createElement("div");
      title.className = "step-title";
      title.innerHTML = `
        <span class="badge">${displayLabel}</span>
        <div>
          <h3>${step.title}</h3>
          <p class="step-info">${step.focus}</p>
        </div>
        <span class="step-meta">${step.tasks.length} 项</span>
      `;

      const progress = document.createElement("div");
      progress.className = "mini-progress";
      progress.innerHTML = `<span id="mini-${step.id}"></span>`;

      const list = document.createElement("div");
      list.className = "task-list";

      step.tasks.forEach((text, idx) => {
        const id = taskId(step.id, idx);
        list.appendChild(createTaskRow(id, text, step.id));
      });

      card.appendChild(title);
      card.appendChild(progress);
      card.appendChild(list);
      container.appendChild(card);
    });
  }

  function renderSprint() {
    const container = document.getElementById("sprintContainer");
    container.innerHTML = "";

    sprintPhases.forEach((phase) => {
      const block = document.createElement("article");
      block.className = "phase";
      block.dataset.phaseId = phase.id;

      const title = document.createElement("h3");
      title.textContent = phase.title;

      const desc = document.createElement("p");
      desc.textContent = phase.goal;

      const list = document.createElement("div");
      list.className = "task-list";

      phase.tasks.forEach((text, idx) => {
        const id = taskId(phase.id, idx);
        list.appendChild(createTaskRow(id, text));
      });

      block.appendChild(title);
      block.appendChild(desc);
      block.appendChild(list);
      container.appendChild(block);
    });
  }

  function renderRisk() {
    const list = document.getElementById("riskList");
    list.innerHTML = "";
    riskItems.forEach((text, idx) => {
      const id = taskId("risk", idx);
      list.appendChild(createTaskRow(id, text));
    });
  }

  function bindTopButtons() {
    document.getElementById("toggleDoneBtn").onclick = () => {
      uiState.hideDone = !uiState.hideDone;
      saveUiState();
      syncHideDone();
      applyTaskFilters();
    };

    document.getElementById("exportBtn").onclick = exportWeeklySummary;

    document.getElementById("resetBtn").onclick = () => {
      if (!confirm("确认把所有任务状态重置为未开始吗？")) return;
      Object.keys(sharedState.tasks).forEach((id) => {
        sharedState.tasks[id].status = "todo";
      });
      sharedState.updatedAt = new Date().toISOString();
      saveSharedCache();
      syncAllUiFromState();
      emitSocket("state:replace", {
        tasks: sharedState.tasks,
        members: sharedState.members,
        actor: getActor()
      });
    };

    document.getElementById("undoBtn").onclick = () => {
      emitSocket("state:undo", { actor: getActor() });
    };
  }

  function bindCollabControls() {
    const currentMemberSelect = document.getElementById("currentMemberSelect");
    const assigneeFilterSelect = document.getElementById("assigneeFilterSelect");
    const dueWeekOnly = document.getElementById("dueWeekOnly");
    const addMemberBtn = document.getElementById("addMemberBtn");
    const memberInput = document.getElementById("memberInput");

    currentMemberSelect.addEventListener("change", () => {
      if (currentMemberSelect.value === ADD_MEMBER_VALUE) {
        const quickName = prompt("请输入成员名字（最多40字）");
        if (quickName !== null) {
          const name = sanitizeName(quickName);
          if (!name) {
            alert("成员名字不能为空。");
          } else {
            if (!sharedState.members.includes(name)) {
              addMemberLocal(name);
              emitSocket("state:patch", {
                type: "member_add",
                member: name,
                actor: getActor()
              });
            }
            uiState.currentMember = name;
            saveUiState();
          }
        }
        syncAllUiFromState();
        return;
      }

      uiState.currentMember = sanitizeName(currentMemberSelect.value);
      saveUiState();
      refreshAssigneeFilterOptions();
      applyTaskFilters();
    });

    assigneeFilterSelect.addEventListener("change", () => {
      uiState.assigneeFilter = assigneeFilterSelect.value;
      saveUiState();
      applyTaskFilters();
    });

    dueWeekOnly.addEventListener("change", () => {
      uiState.dueWeekOnly = dueWeekOnly.checked;
      saveUiState();
      applyTaskFilters();
    });

    addMemberBtn.addEventListener("click", () => {
      const name = sanitizeName(memberInput.value);
      if (!name) return;
      memberInput.value = "";
      if (!addMemberLocal(name)) return;
      syncAllUiFromState();
      emitSocket("state:patch", {
        type: "member_add",
        member: name,
        actor: getActor()
      });
    });

    memberInput.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        addMemberBtn.click();
      }
    });
  }

  function getActor() {
    return uiState.currentMember || "匿名";
  }

  function addMemberLocal(name) {
    if (!name || sharedState.members.includes(name)) return false;
    sharedState.members.push(name);
    sharedState.members.sort((a, b) => a.localeCompare(b, "zh-CN"));
    sharedState.updatedAt = new Date().toISOString();
    saveSharedCache();
    return true;
  }

  function removeMemberLocal(name) {
    if (!name || !sharedState.members.includes(name)) return false;
    sharedState.members = sharedState.members.filter((n) => n !== name);
    Object.values(sharedState.tasks).forEach((task) => {
      if (task.assignee === name) task.assignee = "";
    });
    if (uiState.currentMember === name) {
      uiState.currentMember = "";
      saveUiState();
    }
    sharedState.updatedAt = new Date().toISOString();
    saveSharedCache();
    return true;
  }

  function applyTaskPatchLocal(taskIdValue, changes) {
    const task = getTask(taskIdValue);
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
      const cleanName = sanitizeName(changes.assignee || "");
      next.assignee = cleanName && sharedState.members.includes(cleanName) ? cleanName : "";
    }

    const changed = JSON.stringify(next) !== JSON.stringify(task);
    if (!changed) return { changed: false, applied: {} };

    const applied = {};
    ["status", "priority", "dueDate", "assignee"].forEach((key) => {
      if (next[key] !== task[key]) applied[key] = next[key];
    });

    sharedState.tasks[taskIdValue] = next;
    sharedState.updatedAt = new Date().toISOString();
    saveSharedCache();
    return { changed: true, applied };
  }

  function handleTaskFieldChange(taskIdValue, changes, stepId = null) {
    const { changed, applied } = applyTaskPatchLocal(taskIdValue, changes);
    if (!changed) return;
    syncAllUiFromState(stepId);
    emitSocket("state:patch", {
      type: "task_update",
      taskId: taskIdValue,
      changes: applied,
      actor: getActor()
    });
  }

  function getOrderedTaskItems() {
    const ordered = [];
    steps.forEach((step) => {
      step.tasks.forEach((text, idx) => {
        ordered.push({
          id: taskId(step.id, idx),
          step: getStepDisplayLabel(step),
          stepId: step.id,
          title: step.title,
          text
        });
      });
    });
    sprintPhases.forEach((phase) => {
      phase.tasks.forEach((text, idx) => {
        ordered.push({
          id: taskId(phase.id, idx),
          step: phase.title,
          stepId: phase.id,
          title: "90天运营计划",
          text
        });
      });
    });
    return ordered;
  }

  function pickNextTask(preferUnassigned = false) {
    const ordered = getOrderedTaskItems();
    if (preferUnassigned) {
      const unassigned = ordered.find((item) => {
        const task = getTask(item.id);
        return !isDoneTask(task) && !sanitizeName(task.assignee || "");
      });
      if (unassigned) return unassigned;
    }
    return ordered.find((item) => !isDoneTask(getTask(item.id))) || null;
  }

  function getUpcomingFriday() {
    const now = new Date();
    const day = now.getDay();
    let delta = 5 - day;
    if (delta < 0) delta += 7;
    const due = new Date(now);
    due.setHours(0, 0, 0, 0);
    due.setDate(now.getDate() + delta);
    return due.toISOString().slice(0, 10);
  }

  function scrollToTask(taskIdValue) {
    const row = document.querySelector(`.task[data-task-id='${taskIdValue}']`);
    if (!row) return;
    row.scrollIntoView({ behavior: "smooth", block: "center" });
    row.classList.add("flash");
    setTimeout(() => row.classList.remove("flash"), 700);
  }

  function bindStarterActions() {
    const claimBtn = document.getElementById("claimFirstTaskBtn");
    const dueBtn = document.getElementById("setWeekDueBtn");
    const focusMemberBtn = document.getElementById("focusMemberBtn");
    if (!claimBtn || !dueBtn || !focusMemberBtn) return;

    focusMemberBtn.addEventListener("click", () => {
      const input = document.getElementById("memberInput");
      input?.focus();
      input?.scrollIntoView({ behavior: "smooth", block: "center" });
    });

    claimBtn.addEventListener("click", () => {
      const me = sanitizeName(uiState.currentMember || "");
      if (!me) {
        alert("请先在“我是谁”里选择自己，再领取任务。");
        document.getElementById("currentMemberSelect")?.focus();
        return;
      }

      const next = pickNextTask(true);
      if (!next) {
        alert("当前没有可领取的未完成任务。");
        return;
      }

      const task = getTask(next.id);
      const changes = {};
      if (task.status === "todo") changes.status = "doing";
      if (!sanitizeName(task.assignee || "")) changes.assignee = me;

      if (Object.keys(changes).length) {
        handleTaskFieldChange(next.id, changes, next.stepId);
      }
      scrollToTask(next.id);
    });

    dueBtn.addEventListener("click", () => {
      const target = getOrderedTaskItems().find((item) => {
        const task = getTask(item.id);
        return !isDoneTask(task) && !sanitizeDueDate(task.dueDate || "");
      }) || pickNextTask(false);

      if (!target) {
        alert("当前没有可设置截止日的未完成任务。");
        return;
      }

      const dueDate = getUpcomingFriday();
      handleTaskFieldChange(target.id, { dueDate }, target.stepId);
      scrollToTask(target.id);
    });
  }

  async function postJson(url, payload) {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(payload || {})
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    return response.json();
  }

  async function sendEventNow(event, payload) {
    if (event === "state:patch") {
      return postJson("/api/patch", payload);
    }
    if (event === "state:replace") {
      return postJson("/api/replace", payload);
    }
    if (event === "state:undo") {
      return postJson("/api/undo", payload);
    }
    return null;
  }

  function emitSocket(event, payload) {
    if (!syncConnected) {
      pendingMessages.push({ event, payload });
      setSyncStatus(false, "离线（本地暂存）");
      return;
    }

    sendEventNow(event, payload).catch(() => {
      pendingMessages.push({ event, payload });
      syncConnected = false;
      setSyncStatus(false, "离线（本地暂存）");
    });
  }

  async function flushPending() {
    if (!syncConnected || pendingMessages.length === 0) return;
    while (pendingMessages.length > 0) {
      const item = pendingMessages.shift();
      try {
        await sendEventNow(item.event, item.payload);
      } catch {
        pendingMessages.unshift(item);
        syncConnected = false;
        setSyncStatus(false, "离线（本地暂存）");
        return;
      }
    }
  }

  function replaceSharedState(next) {
    const normalized = normalizeSharedState(next);
    sharedState.tasks = normalized.tasks;
    sharedState.members = normalized.members;
    sharedState.history = normalized.history;
    sharedState.updatedAt = normalized.updatedAt;
    sharedState.undoDepth = normalized.undoDepth;
    ensureKnownTasks();
    saveSharedCache();
    syncAllUiFromState();
  }

  function hasStateData(state) {
    const normalized = normalizeSharedState(state);
    if (normalized.members.length > 0) return true;
    if (normalized.history.length > 0) return true;
    return Object.values(normalized.tasks).some((task) => {
      const item = normalizeTask(task);
      return item.status !== "todo"
        || item.priority !== "medium"
        || Boolean(item.dueDate)
        || Boolean(item.assignee);
    });
  }

  async function syncStateOnConnect(serverRawState) {
    const serverState = normalizeSharedState(serverRawState || {});
    const localState = normalizeSharedState(sharedState);
    const serverHasData = hasStateData(serverState);
    const localHasData = hasStateData(localState);

    if (!serverHasData && localHasData) {
      const restorePayload = {
        tasks: localState.tasks,
        members: localState.members,
        actor: getActor()
      };

      try {
        const restored = await sendEventNow("state:replace", restorePayload);
        if (restored && restored.state) {
          replaceSharedState(restored.state);
        } else {
          replaceSharedState(localState);
        }
        setSyncStatus(true, "实时同步在线（已自动保留历史数据）");
        await flushPending();
        return;
      } catch {
        pendingMessages.push({ event: "state:replace", payload: restorePayload });
        syncConnected = false;
        setSyncStatus(false, "离线（历史数据待同步）");
        return;
      }
    }

    replaceSharedState(serverState);
    await flushPending();
  }

  function connectRealtime() {
    eventSource = new EventSource("/events");
    let bootstrapCompleted = false;

    eventSource.onopen = async () => {
      syncConnected = true;
      setSyncStatus(true, "实时同步在线");
      try {
        const response = await fetch("/api/state");
        const payload = await response.json();
        await syncStateOnConnect(payload?.state);
      } catch {
        // ignore
      } finally {
        bootstrapCompleted = true;
      }
    };

    eventSource.onmessage = (event) => {
      if (!bootstrapCompleted) return;
      try {
        const payload = JSON.parse(event.data);
        if (payload && payload.state) {
          replaceSharedState(payload.state);
        }
      } catch {
        // Ignore malformed SSE payload.
      }
    };

    eventSource.onerror = () => {
      syncConnected = false;
      setSyncStatus(false, "离线（自动重连）");
    };
  }

  function refreshCurrentMemberSelect() {
    const select = document.getElementById("currentMemberSelect");
    const cur = sanitizeName(uiState.currentMember);
    select.innerHTML = "";

    const blank = document.createElement("option");
    blank.value = "";
    blank.textContent = "未设置";
    select.appendChild(blank);

    const quickAdd = document.createElement("option");
    quickAdd.value = ADD_MEMBER_VALUE;
    quickAdd.textContent = "＋添加成员...";
    select.appendChild(quickAdd);

    sharedState.members.forEach((member) => {
      const option = document.createElement("option");
      option.value = member;
      option.textContent = member;
      select.appendChild(option);
    });

    if (!sharedState.members.includes(cur)) {
      uiState.currentMember = "";
      saveUiState();
    }
    select.value = uiState.currentMember;
  }

  function refreshAssigneeFilterOptions() {
    const select = document.getElementById("assigneeFilterSelect");
    const current = uiState.assigneeFilter;
    select.innerHTML = "";

    const options = [
      { value: "all", label: "全部负责人" },
      { value: "me", label: "我的任务" }
    ];
    sharedState.members.forEach((member) => {
      options.push({ value: `member:${member}`, label: member });
    });

    options.forEach((item) => {
      const option = document.createElement("option");
      option.value = item.value;
      option.textContent = item.label;
      select.appendChild(option);
    });

    if (!options.some((item) => item.value === current)) {
      uiState.assigneeFilter = "all";
      saveUiState();
    }
    select.value = uiState.assigneeFilter;
  }

  function refreshMemberList() {
    const wrap = document.getElementById("memberList");
    wrap.innerHTML = "";

    if (sharedState.members.length === 0) {
      const empty = document.createElement("span");
      empty.className = "footer-note";
      empty.textContent = "还没有成员，请先添加。";
      wrap.appendChild(empty);
      return;
    }

    sharedState.members.forEach((member) => {
      const chip = document.createElement("div");
      chip.className = "member-chip";

      const text = document.createElement("span");
      text.textContent = member;

      const removeBtn = document.createElement("button");
      removeBtn.type = "button";
      removeBtn.textContent = "移除";
      removeBtn.addEventListener("click", () => {
        if (!confirm(`确认移除成员 ${member} 吗？该成员负责的任务将被清空负责人。`)) return;
        if (!removeMemberLocal(member)) return;
        syncAllUiFromState();
        emitSocket("state:patch", {
          type: "member_remove",
          member,
          actor: getActor()
        });
      });

      chip.appendChild(text);
      chip.appendChild(removeBtn);
      wrap.appendChild(chip);
    });
  }

  function syncTaskRowsFromState() {
    document.querySelectorAll(".task").forEach((row) => {
      const id = row.dataset.taskId;
      const task = normalizeTask(getTask(id));
      sharedState.tasks[id] = task;

      const statusSelect = row.querySelector("select[data-field='status']");
      const prioritySelect = row.querySelector("select[data-field='priority']");
      const dueInput = row.querySelector("input[data-field='dueDate']");
      const assigneeSelect = row.querySelector("select[data-field='assignee']");

      if (statusSelect) {
        statusSelect.value = task.status;
        applyFieldClass(statusSelect, task.status, "status");
      }
      if (prioritySelect) {
        prioritySelect.value = task.priority;
        applyFieldClass(prioritySelect, task.priority, "priority");
      }
      if (dueInput) {
        dueInput.value = task.dueDate;
      }
      if (assigneeSelect) {
        fillAssigneeSelect(assigneeSelect, task.assignee);
      }

      row.classList.toggle("done", task.status === "done");
      row.dataset.status = task.status;
      row.dataset.assignee = task.assignee;
      row.dataset.duedate = task.dueDate;
    });
  }

  function isDateInCurrentWeek(dateStr) {
    const clean = sanitizeDueDate(dateStr);
    if (!clean) return false;

    const date = new Date(`${clean}T00:00:00`);
    if (Number.isNaN(date.getTime())) return false;

    const now = new Date();
    const mondayOffset = (now.getDay() + 6) % 7;
    const weekStart = new Date(now);
    weekStart.setHours(0, 0, 0, 0);
    weekStart.setDate(now.getDate() - mondayOffset);

    const weekEnd = new Date(weekStart);
    weekEnd.setDate(weekStart.getDate() + 7);

    return date >= weekStart && date < weekEnd;
  }

  function syncHideDone() {
    const btn = document.getElementById("toggleDoneBtn");
    btn.textContent = uiState.hideDone ? "显示全部" : "只看未完成";
    document.getElementById("dueWeekOnly").checked = uiState.dueWeekOnly;
  }

  function applyTaskFilters() {
    const filter = uiState.assigneeFilter;
    const myName = uiState.currentMember;
    let dueWeekOnly = uiState.dueWeekOnly;
    let effectiveFilter = filter;

    if (dueWeekOnly) {
      const hasAnyDueDate = Object.values(sharedState.tasks || {}).some((task) =>
        Boolean(sanitizeDueDate(task && task.dueDate))
      );
      if (!hasAnyDueDate) {
        dueWeekOnly = false;
        uiState.dueWeekOnly = false;
        saveUiState();
        syncHideDone();
      }
    }

    if (effectiveFilter === "me" && !myName) {
      effectiveFilter = "all";
    } else if (effectiveFilter.startsWith("member:")) {
      const target = effectiveFilter.slice("member:".length);
      if (!sharedState.members.includes(target)) {
        effectiveFilter = "all";
      }
    }

    if (effectiveFilter !== uiState.assigneeFilter) {
      uiState.assigneeFilter = effectiveFilter;
      saveUiState();
      refreshAssigneeFilterOptions();
    }

    document.querySelectorAll(".task").forEach((row) => {
      const status = row.dataset.status || "todo";
      const assignee = row.dataset.assignee || "";
      const due = row.dataset.duedate || "";
      let visible = true;

      if (uiState.hideDone && status === "done") visible = false;

      if (effectiveFilter === "me") {
        visible = visible && Boolean(myName) && assignee === myName;
      } else if (effectiveFilter.startsWith("member:")) {
        const target = effectiveFilter.slice("member:".length);
        visible = visible && assignee === target;
      }

      if (dueWeekOnly) {
        visible = visible && isDateInCurrentWeek(due);
      }

      row.style.display = visible ? "" : "none";
    });

    document.querySelectorAll(".step-card, .phase").forEach((block) => {
      const hasVisible = Array.from(block.querySelectorAll(".task")).some((row) => row.style.display !== "none");
      block.style.display = hasVisible ? "" : "none";
    });
  }

  function getAllTaskNodes() {
    return Array.from(document.querySelectorAll(".task"));
  }

  function isDoneTask(task) {
    return sanitizeStatus(task.status) === "done";
  }

  function updateProgress(stepJustCompleted = null) {
    const allTasks = getAllTaskNodes();
    const total = allTasks.length;
    let done = 0;

    allTasks.forEach((node) => {
      const id = node.dataset.taskId;
      const task = getTask(id);
      const isDone = isDoneTask(task);
      node.classList.toggle("done", isDone);
      if (isDone) done += 1;
    });

    const percent = total ? Math.round((done / total) * 100) : 0;
    document.getElementById("doneCount").textContent = String(done);
    document.getElementById("totalCount").textContent = String(total);
    document.getElementById("overallPercent").textContent = `${percent}%`;
    document.getElementById("overallBar").style.width = `${percent}%`;

    let doneSteps = 0;
    steps.forEach((step) => {
      const ids = step.tasks.map((_, idx) => taskId(step.id, idx));
      const hit = ids.filter((id) => isDoneTask(getTask(id))).length;
      const stepPercent = Math.round((hit / ids.length) * 100);
      const mini = document.getElementById(`mini-${step.id}`);
      if (mini) mini.style.width = `${stepPercent}%`;

      const card = document.querySelector(`[data-step-id='${step.id}']`);
      if (card) {
        const wasDone = card.classList.contains("done");
        const nowDone = stepPercent === 100;
        card.classList.toggle("done", nowDone);
        if (!wasDone && nowDone && step.id === stepJustCompleted) {
          card.classList.add("flash");
          setTimeout(() => card.classList.remove("flash"), 850);
        }
      }

      if (stepPercent === 100) doneSteps += 1;
    });

    document.getElementById("doneSteps").textContent = `${doneSteps} / ${steps.length}`;

    const sprintIds = sprintPhases.flatMap((phase) => phase.tasks.map((_, idx) => taskId(phase.id, idx)));
    const sprintDone = sprintIds.filter((id) => isDoneTask(getTask(id))).length;
    const sprintPercent = Math.round((sprintDone / sprintIds.length) * 100);
    document.getElementById("sprintPercent").textContent = `${sprintPercent}%`;

    const countryKeys = ["s3-t1", "s3-t2", "s3-t3"];
    const countryDone = countryKeys.filter((id) => isDoneTask(getTask(id))).length;
    const countryReady = Math.round((countryDone / countryKeys.length) * 100);
    document.getElementById("countryReady").textContent = `${countryReady}%`;

    updateNextAction();
    updateMilestone(percent, doneSteps);
  }

  function updateNextAction() {
    const nextTitle = document.getElementById("nextActionTitle");
    const nextBody = document.getElementById("nextActionBody");
    const starterWrap = document.getElementById("starterWrap");
    const ordered = getOrderedTaskItems();
    const doneCount = ordered.filter((item) => isDoneTask(getTask(item.id))).length;

    const next = ordered.find((item) => !isDoneTask(getTask(item.id)));
    if (!next) {
      nextTitle.textContent = "全部主线任务已完成";
      nextBody.textContent = "你们可以进入最终答辩材料排版与角色演练。";
      if (starterWrap) starterWrap.style.display = "none";
      return;
    }

    if (starterWrap) {
      starterWrap.style.display = doneCount === 0 ? "grid" : "none";
    }

    const task = getTask(next.id);
    const owner = task.assignee ? `（负责人：${task.assignee}）` : "";
    nextTitle.textContent = doneCount === 0
      ? `启动动作：${next.step} · ${next.title}`
      : `${next.step} · ${next.title}`;
    nextBody.textContent = doneCount === 0
      ? `先完成「${next.text}」${owner}。你可以直接用下方按钮一键领取和设置截止日。`
      : `${next.text}${owner}`;
  }

  function updateMilestone(percent, doneSteps) {
    const box = document.getElementById("milestoneBox");
    box.className = "milestone";
    const totalSteps = steps.length;
    const goodThreshold = Math.max(1, totalSteps - 1);
    const warnThreshold = 45;
    if (doneSteps >= goodThreshold) {
      box.classList.add("good");
      box.textContent = `里程碑：已进入收尾冲刺区（>=${goodThreshold}章完成），优先做证据链、财务口径与统稿一致性。`;
    } else if (percent >= warnThreshold) {
      box.classList.add("warn");
      box.textContent = "里程碑：进度过半，下一阶段重点推进国际策略、财务计划与结论落地。";
    } else {
      box.classList.add("risk");
      box.textContent = "里程碑：当前仍在前半段，请优先完成 Opportunity + Market + Marketing 三章。";
    }
  }

  function formatFieldValue(field, value) {
    if (field === "status") return STATUS_LABELS[sanitizeStatus(value)] || "未开始";
    if (field === "priority") return PRIORITY_LABELS[sanitizePriority(value)] || "中";
    if (field === "dueDate") return sanitizeDueDate(value) || "未设置";
    if (field === "assignee") return sanitizeName(value || "") || "未分配";
    return String(value ?? "");
  }

  function formatHistoryText(entry) {
    const actor = entry.actor || "匿名";

    if (entry.kind === "task_update") {
      const taskInfo = taskDefMap[entry.taskId] || { text: entry.taskId || "任务" };
      const changes = [];
      Object.entries(entry.changes || {}).forEach(([field, change]) => {
        if (!change || typeof change !== "object") return;
        const from = formatFieldValue(field, change.from);
        const to = formatFieldValue(field, change.to);
        changes.push(`${field}: ${from} → ${to}`);
      });
      return `${actor} 更新「${taskInfo.text}」${changes.length ? `（${changes.join("；")}）` : ""}`;
    }

    if (entry.kind === "member_add") {
      return `${actor} 添加成员：${entry.message || ""}`;
    }

    if (entry.kind === "member_remove") {
      return `${actor} 移除成员：${entry.message || ""}`;
    }

    if (entry.kind === "undo") {
      return `${actor} 执行撤销：${entry.message || "恢复上一操作"}`;
    }

    if (entry.kind === "replace") {
      return `${actor} 执行批量重置`;
    }

    return `${actor} 更新了项目状态`;
  }

  function renderHistory() {
    const list = document.getElementById("historyList");
    list.innerHTML = "";

    if (!Array.isArray(sharedState.history) || sharedState.history.length === 0) {
      const empty = document.createElement("div");
      empty.className = "history-item";
      empty.innerHTML = `<div class="history-main">暂无操作日志</div>`;
      list.appendChild(empty);
      return;
    }

    sharedState.history.slice(0, 120).forEach((entry) => {
      const item = document.createElement("div");
      item.className = "history-item";

      const main = document.createElement("div");
      main.className = "history-main";
      main.textContent = formatHistoryText(entry);

      const meta = document.createElement("div");
      meta.className = "history-meta";
      const time = new Date(entry.at || Date.now());
      meta.textContent = Number.isNaN(time.getTime())
        ? "时间未知"
        : time.toLocaleString("zh-CN", { hour12: false });

      item.appendChild(main);
      item.appendChild(meta);
      list.appendChild(item);
    });
  }

  function updateUndoButton() {
    const btn = document.getElementById("undoBtn");
    const depth = Number(sharedState.undoDepth || 0);
    btn.disabled = depth <= 0;
    btn.textContent = `撤销上一步 (${depth})`;
  }

  function syncAllUiFromState(stepJustCompleted = null) {
    ensureKnownTasks();
    syncHideDone();
    refreshCurrentMemberSelect();
    refreshAssigneeFilterOptions();
    refreshMemberList();
    syncTaskRowsFromState();
    updateProgress(stepJustCompleted);
    applyTaskFilters();
    renderHistory();
    updateUndoButton();
  }

  function exportWeeklySummary() {
    const lines = [];
    lines.push("# IMS 项目周进展");
    lines.push("");
    lines.push(`- 总完成度：${document.getElementById("overallPercent").textContent}`);
    lines.push(`- 完成任务：${document.getElementById("doneCount").textContent}/${document.getElementById("totalCount").textContent}`);
    lines.push(`- 完成章节：${document.getElementById("doneSteps").textContent}`);
    if (uiState.currentMember) lines.push(`- 汇报人：${uiState.currentMember}`);
    lines.push("");
    lines.push("## 已完成");

    const doneDefs = Object.entries(taskDefMap)
      .filter(([id]) => isDoneTask(getTask(id)))
      .map(([id, def]) => ({ id, ...def, task: getTask(id) }));

    if (doneDefs.length === 0) {
      lines.push("- （暂无）");
    } else {
      doneDefs.forEach((item) => {
        const owner = item.task.assignee ? `负责人:${item.task.assignee}` : "负责人:未分配";
        lines.push(`- ${item.text}（${owner}）`);
      });
    }

    lines.push("");
    lines.push("## 下周优先");

    const todoDefs = Object.entries(taskDefMap)
      .filter(([id]) => !isDoneTask(getTask(id)))
      .map(([id, def]) => ({ id, ...def, task: getTask(id) }))
      .sort((a, b) => {
        const ad = a.task.dueDate || "9999-99-99";
        const bd = b.task.dueDate || "9999-99-99";
        return ad.localeCompare(bd);
      })
      .slice(0, 5);

    if (todoDefs.length === 0) {
      lines.push("- 进入答辩与展示准备");
    } else {
      todoDefs.forEach((item) => {
        const due = item.task.dueDate ? `截止:${item.task.dueDate}` : "截止:未设置";
        const owner = item.task.assignee ? `负责人:${item.task.assignee}` : "负责人:未分配";
        lines.push(`- ${item.text}（${owner}，${due}）`);
      });
    }

    const text = lines.join("\n");
    navigator.clipboard.writeText(text).then(() => {
      alert("本周进展已复制到剪贴板，可直接发到群里。");
    }).catch(() => {
      prompt("复制下面的进展文本：", text);
    });
  }

  render();
})();
