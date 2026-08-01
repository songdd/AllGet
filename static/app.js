(function() {
  var urlInput = document.getElementById("urlInput");
  var btnAdd = document.getElementById("btnAdd");
  var taskList = document.getElementById("taskList");
  var emptyState = document.getElementById("emptyState");
  var activeCount = document.getElementById("activeCount");
  var totalSpeed = document.getElementById("totalSpeed");
  var torrentInput = document.getElementById("torrentFileInput");
  var ws;

  var tasks = {};

  /* Format helpers */
  function fmtSize(bytes) {
    if (bytes === 0 || bytes == null) return "0 B";
    var units = ["B", "KB", "MB", "GB", "TB"];
    var i = Math.floor(Math.log(bytes) / Math.log(1024));
    i = Math.min(i, units.length - 1);
    return (bytes / Math.pow(1024, i)).toFixed(i > 0 ? 1 : 0) + " " + units[i];
  }

  function fmtSpeed(bytesPerSec) {
    return fmtSize(bytesPerSec) + "/s";
  }

  function fmtPercent(p) {
    return (p * 100).toFixed(1) + "%";
  }

  /* WebSocket */
  function connect() {
    var proto = location.protocol === "https:" ? "wss:" : "ws:";
    ws = new WebSocket(proto + "//" + location.host + "/ws");

    ws.onmessage = function(e) {
      var msg = JSON.parse(e.data);
      if (msg.type === "task_update") {
        handleTaskUpdate(msg.task);
      }
    };

    ws.onclose = function() {
      setTimeout(connect, 2000);
    };

    ws.onerror = function() {
      ws.close();
    };
  }

  function handleTaskUpdate(task) {
    tasks[task.id] = task;
    render();
  }

  /* API calls */
  async function api(path, opts) {
    var res = await fetch(path, opts);
    return res.json();
  }

  async function addDownload() {
    var url = urlInput.value.trim();
    if (!url) return;

    var form = new FormData();
    form.append("url", url);

    try {
      var data = await api("/api/tasks", { method: "POST", body: form });
      if (data.error) {
        alert("Error: " + data.error);
      }
      urlInput.value = "";
    } catch (err) {
      alert("Failed to add task: " + err.message);
    }
  }

  async function pauseTask(id) {
    await api("/api/tasks/" + id + "/pause", { method: "POST" });
  }

  async function resumeTask(id) {
    await api("/api/tasks/" + id + "/resume", { method: "POST" });
  }

  async function stopTask(id) {
    await api("/api/tasks/" + id + "/stop", { method: "POST" });
  }

  async function deleteTask(id) {
    await api("/api/tasks/" + id, { method: "DELETE" });
    delete tasks[id];
    render();
  }

  async function uploadTorrent(file) {
    var form = new FormData();
    form.append("file", file);
    try {
      var data = await api("/api/tasks/upload-torrent", { method: "POST", body: form });
      if (data.error) {
        alert("Error: " + data.error);
      }
    } catch (err) {
      alert("Failed to upload torrent: " + err.message);
    }
  }

  /* Render */
  function render() {
    var ids = Object.keys(tasks);
    var hasTasks = ids.length > 0;

    if (hasTasks) {
      emptyState.style.display = "none";
    } else {
      emptyState.style.display = "";
    }

    // Update stats
    var active = 0, speed = 0;
    ids.forEach(function(id) {
      var t = tasks[id];
      if (t.status === "downloading") { active++; speed += t.download_speed || 0; }
    });
    activeCount.textContent = active + " active";
    totalSpeed.textContent = fmtSpeed(speed);

    // Build task cards
    var existingCards = {};
    taskList.querySelectorAll(".task-card").forEach(function(el) {
      existingCards[el.dataset.id] = el;
    });

    var fragment = document.createDocumentFragment();

    ids.forEach(function(id) {
      var t = tasks[id];
      var card = existingCards[id];
      if (!card) {
        card = document.createElement("div");
        card.className = "task-card";
        card.dataset.id = id;
      }
      card.innerHTML = buildCardHTML(t);

      // Bind event listeners
      var pauseBtn = card.querySelector(".btn-pause");
      var resumeBtn = card.querySelector(".btn-resume");
      var stopBtn = card.querySelector(".btn-stop");
      var deleteBtn = card.querySelector(".btn-delete");

      if (pauseBtn) pauseBtn.onclick = function() { pauseTask(id); };
      if (resumeBtn) resumeBtn.onclick = function() { resumeTask(id); };
      if (stopBtn) stopBtn.onclick = function() { stopTask(id); };
      if (deleteBtn) deleteBtn.onclick = function() { deleteTask(id); };

      fragment.appendChild(card);
    });

    taskList.innerHTML = "";
    taskList.appendChild(fragment);
    if (!hasTasks) {
      taskList.appendChild(emptyState);
    }
  }

  function buildCardHTML(t) {
    var statusClass = "status-" + t.status;
    var progressFillClass = "";
    if (t.status === "completed") progressFillClass = "complete";
    if (t.status === "failed") progressFillClass = "failed";

    var statusLabel = t.status.charAt(0).toUpperCase() + t.status.slice(1);
    var progressPct = Math.min(100, Math.max(0, (t.progress || 0) * 100)).toFixed(1);
    var speedText = t.status === "downloading" ? fmtSpeed(t.download_speed || 0) : "";
    var sizeText = fmtSize(t.total_bytes || 0);
    var downText = fmtSize(t.downloaded_bytes || 0);

    var actionButtons = "";
    if (t.status === "downloading") {
      actionButtons += '<button class="btn btn-sm btn-secondary btn-pause" title="Pause"><svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="4" width="4" height="16"/><rect x="14" y="4" width="4" height="16"/></svg></button>';
      actionButtons += '<button class="btn btn-sm btn-secondary btn-stop" title="Stop"><svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><rect x="4" y="4" width="16" height="16" rx="2"/></svg></button>';
    } else if (t.status === "paused") {
      actionButtons += '<button class="btn btn-sm btn-primary btn-resume" title="Resume"><svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><polygon points="5 3 19 12 5 21"/></svg></button>';
      actionButtons += '<button class="btn btn-sm btn-secondary btn-stop" title="Stop"><svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><rect x="4" y="4" width="16" height="16" rx="2"/></svg></button>';
    } else if (t.status === "queued") {
      actionButtons += '<button class="btn btn-sm btn-secondary btn-stop" title="Stop"><svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><rect x="4" y="4" width="16" height="16" rx="2"/></svg></button>';
    }
    actionButtons += '<button class="btn btn-sm btn-danger btn-delete" title="Delete"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 01-2 2H8a2 2 0 01-2-2L5 6"/><line x1="10" y1="11" x2="10" y2="17"/><line x1="14" y1="11" x2="14" y2="17"/></svg></button>';

    var linkIcon = getLinkIcon(t.link_type);
    var errorHTML = t.error ? '<div class="error-text">' + escapeHTML(t.error) + '</div>' : "";

    return (
      '<div class="task-header">' +
        '<div class="task-icon">' + linkIcon + '</div>' +
        '<div class="task-info">' +
          '<div class="task-name" title="' + escapeHTML(t.filename || t.url) + '">' + escapeHTML(t.filename || t.url) + '</div>' +
          '<div class="task-meta">' +
            '<span class="link-tag">' + escapeHTML(t.link_type) + '</span>' +
            '<span class="status-badge ' + statusClass + '">' + statusLabel + '</span>' +
            (speedText ? '<span>' + speedText + '</span>' : '') +
            '<span>' + downText + ' / ' + sizeText + '</span>' +
          '</div>' +
        '</div>' +
        '<div class="task-actions">' + actionButtons + '</div>' +
      '</div>' +
      '<div class="progress-wrap">' +
        '<div class="progress-bar">' +
          '<div class="progress-fill ' + progressFillClass + '" style="width:' + progressPct + '%"></div>' +
        '</div>' +
        '<div class="progress-text">' +
          '<span>' + progressPct + '%</span>' +
          '<span>' + downText + ' / ' + sizeText + '</span>' +
        '</div>' +
      '</div>' +
      errorHTML
    );
  }

  function getLinkIcon(type) {
    var icons = {
      "http": '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10 13a5 5 0 007.54.54l3-3a5 5 0 00-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 00-7.54-.54l-3 3a5 5 0 007.07 7.07l1.71-1.71"/></svg>',
      "https": '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>',
      "magnet": '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M6 3v18l6-4 6 4V3"/></svg>',
      "torrent": '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>',
      "ed2k": '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="2" width="20" height="8" rx="2"/><rect x="2" y="14" width="20" height="8" rx="2"/><line x1="6" y1="6" x2="6" y2="6.01"/><line x1="6" y1="18" x2="6" y2="18.01"/></svg>',
      "unknown": '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>'
    };
    return icons[type] || icons.unknown;
  }

  function escapeHTML(str) {
    if (!str) return "";
    return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  }

  /* Event listeners */
  btnAdd.addEventListener("click", addDownload);
  urlInput.addEventListener("keydown", function(e) {
    if (e.key === "Enter") addDownload();
  });

  torrentInput.addEventListener("change", function() {
    if (this.files && this.files[0]) {
      uploadTorrent(this.files[0]);
      this.value = "";
    }
  });

  // Paste from clipboard on focus
  urlInput.addEventListener("focus", async function() {
    if (!urlInput.value) {
      try {
        var text = await navigator.clipboard.readText();
        if (text && text.match(/^(https?:\/\/|magnet:|ed2k:\/\/)/i)) {
          urlInput.value = text;
        }
      } catch(e) {}
    }
  });

  // Initial load
  connect();

  // Load existing tasks
  fetch("/api/tasks")
    .then(function(r) { return r.json(); })
    .then(function(data) {
      if (data.tasks) {
        data.tasks.forEach(function(t) { tasks[t.id] = t; });
        render();
      }
    });
})();