let currentJob = null;
let pollTimer = null;

const $ = (id) => document.getElementById(id);

function collectForm() {
  const fd = new FormData($("configForm"));
  const get = (name) => String(fd.get(name) || "").trim();
  return {
    source: {
      host: get("source.host"),
      user: get("source.user"),
      port: Number(get("source.port") || 22),
      device: get("source.device"),
      key: String(fd.get("source.key") || "")
    },
    target: {
      host: get("target.host"),
      user: get("target.user"),
      port: Number(get("target.port") || 22),
      device: get("target.device"),
      key: String(fd.get("target.key") || "")
    },
    socketPort: Number(get("socketPort") || 19090),
    minor: Number(get("minor") || 0)
  };
}

function humanBytes(value) {
  const n = Number(value || 0);
  const units = ["B", "KB", "MB", "GB", "TB"];
  let v = n;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(i ? 1 : 0)} ${units[i]}`;
}

function humanSpeed(value) {
  return `${humanBytes(value)}/s`;
}

async function post(url, body = {}) {
  const res = await fetch(url, {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify(body)
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || "request failed");
  return data;
}

async function probe(which) {
  const cfg = collectForm()[which];
  $("probeOutput").textContent = `正在探测 ${which} ${cfg.host} ...`;
  try {
    const data = await post("/api/probe", cfg);
    $("probeOutput").textContent = data.output || "(无输出)";
  } catch (err) {
    $("probeOutput").textContent = `探测失败：${err.message}`;
  }
}

function setButtons(job) {
  const ready = job && job.status === "ready";
  const tracking = job && job.status === "tracking";
  const running = job && job.status === "running";
  $("startFull").disabled = !ready || running;
  $("startInc").disabled = !tracking || running;
  $("refreshRemote").disabled = !job || running;
  $("cleanup").disabled = !(job && (tracking || ready || job.status === "failed"));
}

function render(job) {
  $("jobStatus").textContent = `${job.status} / ${job.phase}`;
  $("jobId").textContent = job.id;
  $("phase").textContent = job.phase;
  $("speed").textContent = humanSpeed(job.metrics.speed_bps);
  $("bytesDone").textContent = humanBytes(job.metrics.bytes_done);
  $("changedBlocks").textContent = job.metrics.nr_changed_blocks || "-";
  $("rangeCount").textContent = job.metrics.ranges_count || "-";
  const lt = job.metrics.last_transfer || {};
  $("lastTransfer").textContent = lt.type ? `${lt.type} ${humanBytes(lt.bytes)} / ${Number(lt.elapsed || 0).toFixed(1)}s` : "-";
  $("logCount").textContent = job.log_count || (job.logs || []).length || "-";
  $("ranges").textContent = (job.metrics.ranges_preview || []).join("\n") || "等待增量同步...";
  $("logs").textContent = (job.logs || []).join("\n") || "等待任务...";
  $("logs").scrollTop = $("logs").scrollHeight;
  setButtons(job);
}

async function poll() {
  if (!currentJob) return;
  try {
    const res = await fetch(`/api/jobs/${currentJob}`);
    const job = await res.json();
    render(job);
  } catch (err) {
    $("logs").textContent += `\n轮询失败：${err.message}`;
  }
}

async function loadJobs() {
  const res = await fetch("/api/jobs");
  const data = await res.json();
  const jobs = data.jobs || [];
  if (!jobs.length) {
    $("jobList").textContent = "暂无历史任务";
    return;
  }
  $("jobList").innerHTML = "";
  for (const job of jobs) {
    const item = document.createElement("div");
    item.className = "job-item";
    const left = document.createElement("div");
    left.innerHTML = `<strong>${job.id} · ${job.status}/${job.phase}</strong><span>${job.updated_at} · ${job.source?.host || "-"} ${job.source?.device || ""} → ${job.target?.host || "-"} ${job.target?.device || ""}</span>`;
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "ghost";
    btn.textContent = "载入";
    btn.addEventListener("click", () => {
      currentJob = job.id;
      $("jobId").textContent = currentJob;
      if (pollTimer) clearInterval(pollTimer);
      pollTimer = setInterval(poll, 1500);
      poll();
    });
    item.append(left, btn);
    $("jobList").appendChild(item);
  }
}

$("probeSource").addEventListener("click", () => probe("source"));
$("probeTarget").addEventListener("click", () => probe("target"));

$("configForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  $("jobStatus").textContent = "创建中";
  try {
    const data = await post("/api/jobs", collectForm());
    currentJob = data.id;
    $("jobId").textContent = currentJob;
    if (pollTimer) clearInterval(pollTimer);
    pollTimer = setInterval(poll, 1500);
    poll();
    loadJobs();
  } catch (err) {
    $("jobStatus").textContent = "创建失败";
    $("logs").textContent = err.message;
  }
});

$("adoptRemote").addEventListener("click", async () => {
  $("jobStatus").textContent = "接管中";
  try {
    const data = await post("/api/adopt", collectForm());
    currentJob = data.id;
    $("jobId").textContent = currentJob;
    if (pollTimer) clearInterval(pollTimer);
    pollTimer = setInterval(poll, 1500);
    poll();
    loadJobs();
  } catch (err) {
    $("jobStatus").textContent = "接管失败";
    $("logs").textContent = err.message;
  }
});

$("startFull").addEventListener("click", async () => {
  if (!currentJob) return;
  await post(`/api/jobs/${currentJob}/start-full`);
  poll();
});

$("startInc").addEventListener("click", async () => {
  if (!currentJob) return;
  await post(`/api/jobs/${currentJob}/start-incremental`);
  poll();
});

$("cleanup").addEventListener("click", async () => {
  if (!currentJob) return;
  await post(`/api/jobs/${currentJob}/cleanup`);
  poll();
});

$("refreshRemote").addEventListener("click", async () => {
  if (!currentJob) return;
  await post(`/api/jobs/${currentJob}/refresh-remote`);
  poll();
});

$("reloadJobs").addEventListener("click", loadJobs);

loadJobs();
