import argparse
import json
import mimetypes
import os
import shutil
import sys
import threading
import uuid
from datetime import datetime, timedelta
from typing import Any, Optional

from flask import (
    Flask,
    abort,
    jsonify,
    render_template_string,
    request,
    send_file,
    url_for,
)

from . import collect, subconverter
from .logger import logger


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    return dt.replace(microsecond=0).isoformat() + "Z"


class CollectManager:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._timer: Optional[threading.Timer] = None
        self._running = False

        self._history_limit = max(1, _env_int("DASHBOARD_HISTORY_LIMIT", 20))

        self.targets = self._resolve_targets()
        self.primary_target = self.targets[0]

        self.delay = _env_int("DASHBOARD_DELAY", 5000)
        self.num_threads = _env_int("DASHBOARD_THREADS", 32)
        self.skip_checks = _env_bool("DASHBOARD_SKIP_CHECKS", False)
        self.chuck = _env_bool("DASHBOARD_CHUCK", False)
        self.refresh = _env_bool("DASHBOARD_REFRESH", False)
        self.overwrite = _env_bool("DASHBOARD_OVERWRITE", False)
        self.easygoing = _env_bool("DASHBOARD_EASYGOING", False)
        self.full_config = _env_bool("DASHBOARD_FULL_CONFIG", False)
        self.show_progress = _env_bool("DASHBOARD_SHOW_PROGRESS", False)
        self.invisible = not self.show_progress
        self.life_hours = _env_int("DASHBOARD_LIFE", 0)
        self.flow_gb = _env_int("DASHBOARD_FLOW", 0)
        self.pages = _env_int("DASHBOARD_PAGES", sys.maxsize)
        self.test_url = os.getenv("DASHBOARD_TEST_URL", "https://www.google.com/generate_204")
        self.vitiate = _env_bool("DASHBOARD_VITIATE", False)

        self.interval_minutes = max(0.0, _env_float("DASHBOARD_INTERVAL", 0.0))
        self.next_run_at: Optional[datetime] = None
        self.last_run_at: Optional[datetime] = None
        self.last_success: Optional[bool] = None
        self.last_message = ""
        self.last_duration: Optional[float] = None

        self.export_dir = os.path.join(collect.DATA_BASE, "exports")
        os.makedirs(self.export_dir, exist_ok=True)
        self.history_file = os.path.join(self.export_dir, "runs.json")

        self.runs: list[dict[str, Any]] = []
        self._load_state()

        with self._lock:
            if self.interval_minutes > 0:
                self._schedule_locked()

    def _resolve_targets(self) -> list[str]:
        raw_targets = os.getenv("DASHBOARD_TARGETS")
        if raw_targets:
            candidates = [x.strip().lower() for x in raw_targets.split(",") if x.strip()]
        else:
            candidates = []

        valid = [x for x in candidates if subconverter.get_filename(x)]
        if not valid:
            valid = ["clash"]
        return valid

    def _build_args(self) -> argparse.Namespace:
        return argparse.Namespace(
            all=self.full_config,
            chuck=self.chuck,
            delay=self.delay,
            easygoing=self.easygoing,
            flow=self.flow_gb,
            gist=os.getenv("GIST_LINK", ""),
            invisible=self.invisible,
            key=os.getenv("GIST_PAT", ""),
            life=self.life_hours,
            num=self.num_threads,
            overwrite=self.overwrite,
            pages=self.pages,
            refresh=self.refresh,
            skip=self.skip_checks,
            targets=self.targets,
            url=self.test_url,
            vitiate=self.vitiate,
            yourself=os.getenv("CUSTOMIZE_LINK", ""),
        )

    def _load_state(self) -> None:
        if not os.path.exists(self.history_file):
            return

        try:
            with open(self.history_file, "r", encoding="utf8") as f:
                data = json.load(f)
        except Exception as exc:
            logger.warning("failed to load dashboard state: %s", exc)
            return

        interval = data.get("interval")
        try:
            if interval is not None:
                self.interval_minutes = max(0.0, float(interval))
        except (TypeError, ValueError):
            logger.warning("invalid interval value in state file: %s", interval)

        runs = data.get("runs", [])
        for item in runs:
            uuid_value = item.get("uuid")
            filename = item.get("filename")
            created = item.get("created_at")
            target = item.get("target") or self.primary_target
            if not uuid_value or not filename:
                continue

            path = os.path.join(self.export_dir, filename)
            if not os.path.exists(path):
                continue

            record = {
                "uuid": uuid_value,
                "filename": filename,
                "target": target,
                "created_at": created,
                "path": path,
                "size": os.path.getsize(path),
            }
            self.runs.append(record)

        self.runs = self.runs[: self._history_limit]

    def _persist_state(self) -> None:
        runs = []
        for item in self.runs:
            path = item.get("path") or os.path.join(self.export_dir, item["filename"])
            if not os.path.exists(path):
                continue
            runs.append(
                {
                    "uuid": item["uuid"],
                    "filename": item["filename"],
                    "target": item["target"],
                    "created_at": item.get("created_at"),
                    "size": item.get("size", 0),
                }
            )

        payload = {
            "interval": self.interval_minutes,
            "runs": runs,
        }

        try:
            with open(self.history_file, "w", encoding="utf8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception as exc:
            logger.warning("failed to persist dashboard state: %s", exc)

    def _refresh_runs_locked(self) -> None:
        changed = False
        for item in list(self.runs):
            path = item.get("path") or os.path.join(self.export_dir, item["filename"])
            if os.path.exists(path):
                item["path"] = path
                item["size"] = os.path.getsize(path)
            else:
                self.runs.remove(item)
                changed = True
        if changed:
            self._persist_state()

    def _cancel_timer_locked(self) -> None:
        if self._timer:
            self._timer.cancel()
            self._timer = None
        self.next_run_at = None

    def _schedule_locked(self) -> None:
        self._cancel_timer_locked()
        if self.interval_minutes <= 0:
            return

        delay = max(0.0, self.interval_minutes * 60.0)
        self.next_run_at = datetime.utcnow() + timedelta(seconds=delay)
        self._timer = threading.Timer(delay, self._on_timer)
        self._timer.daemon = True
        self._timer.start()

    def _on_timer(self) -> None:
        started, message = self.start_run(trigger="schedule")
        if not started:
            logger.info("scheduled collect was skipped: %s", message)
            with self._lock:
                if self.interval_minutes > 0:
                    self._schedule_locked()

    def start_run(self, trigger: str = "manual") -> tuple[bool, str]:
        with self._lock:
            if self._running:
                return False, "collect task is already running"

            logger.info("starting collect task (trigger=%s)", trigger)
            self._running = True
            self._cancel_timer_locked()
            thread = threading.Thread(target=self._execute_run, args=(trigger,), daemon=True)
            thread.start()
            return True, "collect task started"

    def _execute_run(self, trigger: str) -> None:
        started_at = datetime.utcnow()
        record: Optional[dict[str, Any]] = None
        success = False
        message = ""

        try:
            args = self._build_args()
            collect.aggregate(args)
            record = self._snapshot_primary_output()
            if record is None:
                message = "collect finished but no output was produced"
            else:
                success = True
                message = record["uuid"]
        except SystemExit as exc:
            message = f"collect exited with status {exc.code}"
            logger.warning("collect exited early during %s trigger with code %s", trigger, exc.code)
        except Exception as exc:
            message = str(exc)
            logger.exception("collect execution failed (%s): %s", trigger, exc)
        finally:
            finished_at = datetime.utcnow()
            duration = (finished_at - started_at).total_seconds()
            with self._lock:
                self._running = False
                self.last_run_at = finished_at
                self.last_success = success
                self.last_message = message
                self.last_duration = duration

                if record is not None:
                    self.runs.insert(0, record)
                    self._limit_history_locked()

                self._persist_state()

                if self.interval_minutes > 0:
                    self._schedule_locked()

            logger.info(
                "collect task finished (trigger=%s, success=%s, duration=%.2fs)",
                trigger,
                success,
                duration,
            )

    def _snapshot_primary_output(self) -> Optional[dict[str, Any]]:
        filename = subconverter.get_filename(self.primary_target)
        if not filename:
            logger.error("unsupported target for dashboard export: %s", self.primary_target)
            return None

        source = os.path.join(collect.DATA_BASE, filename)
        if not os.path.exists(source):
            logger.warning("expected output file %s is missing", source)
            return None

        run_uuid = str(uuid.uuid4())
        extension = os.path.splitext(filename)[1]
        dest_filename = f"{run_uuid}{extension}"
        destination = os.path.join(self.export_dir, dest_filename)

        shutil.copy2(source, destination)
        size = os.path.getsize(destination)
        created_at = datetime.utcnow().isoformat() + "Z"

        record = {
            "uuid": run_uuid,
            "filename": dest_filename,
            "target": self.primary_target,
            "path": destination,
            "created_at": created_at,
            "size": size,
        }
        return record

    def _limit_history_locked(self) -> None:
        if len(self.runs) <= self._history_limit:
            return

        overflow = self.runs[self._history_limit :]
        self.runs = self.runs[: self._history_limit]

        for item in overflow:
            path = item.get("path") or os.path.join(self.export_dir, item["filename"])
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    logger.warning("failed to remove expired snapshot: %s", path)

    def update_interval(self, minutes: float) -> tuple[float, Optional[datetime]]:
        with self._lock:
            self.interval_minutes = max(0.0, minutes)
            if self.interval_minutes <= 0:
                self._cancel_timer_locked()
            else:
                self._schedule_locked()
            self._persist_state()
            return self.interval_minutes, self.next_run_at

    def get_state(self) -> dict[str, Any]:
        with self._lock:
            self._refresh_runs_locked()
            return {
                "running": self._running,
                "interval": self.interval_minutes,
                "next_run": _iso(self.next_run_at),
                "last_run": _iso(self.last_run_at),
                "last_success": self.last_success,
                "last_message": self.last_message,
                "last_duration": self.last_duration,
                "target": self.primary_target,
                "results": [
                    {
                        "uuid": item["uuid"],
                        "filename": item["filename"],
                        "target": item["target"],
                        "created_at": item.get("created_at"),
                        "size": item.get("size", 0),
                    }
                    for item in self.runs
                ],
            }

    def get_file(self, run_id: str) -> tuple[str, Optional[str]]:
        with self._lock:
            for item in self.runs:
                if item["uuid"] == run_id:
                    path = item.get("path") or os.path.join(self.export_dir, item["filename"])
                    if os.path.exists(path):
                        return path, item["filename"]
        return "", None


manager = CollectManager()
app = Flask(__name__)


HTML_TEMPLATE = """<!doctype html>
<html lang=\"zh-CN\">
<head>
  <meta charset=\"utf-8\">
  <meta http-equiv=\"X-UA-Compatible\" content=\"IE=edge\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Aggregator Dashboard</title>
  <style>
    :root {
      color-scheme: light dark;
    }

    body {
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
      background: #f3f4f6;
      color: #1f2937;
    }

    .container {
      max-width: 960px;
      margin: 0 auto;
      padding: 32px 16px 48px;
    }

    .card {
      background: white;
      border-radius: 16px;
      padding: 28px;
      margin-bottom: 32px;
      box-shadow: 0 20px 45px rgba(15, 23, 42, 0.08);
      border: 1px solid rgba(15, 23, 42, 0.05);
    }

    h1 {
      margin: 0 0 12px;
      font-size: 28px;
      font-weight: 700;
    }

    h2 {
      margin-top: 0;
      font-size: 22px;
      font-weight: 600;
    }

    .muted {
      color: #6b7280;
      font-size: 15px;
    }

    .controls {
      display: flex;
      flex-wrap: wrap;
      gap: 16px;
      align-items: flex-end;
      margin: 28px 0 12px;
    }

    button {
      background: linear-gradient(135deg, #2563eb, #1d4ed8);
      border: none;
      color: #fff;
      padding: 12px 22px;
      border-radius: 12px;
      cursor: pointer;
      font-size: 15px;
      font-weight: 600;
      box-shadow: 0 10px 25px rgba(37, 99, 235, 0.35);
      transition: transform 0.1s ease, box-shadow 0.2s ease;
    }

    button:hover {
      transform: translateY(-1px);
      box-shadow: 0 15px 35px rgba(37, 99, 235, 0.45);
    }

    button:disabled {
      background: #bfdbfe;
      color: #1e3a8a;
      cursor: not-allowed;
      box-shadow: none;
      transform: none;
    }

    button.secondary {
      background: #f87171;
      box-shadow: 0 10px 25px rgba(248, 113, 113, 0.35);
    }

    label {
      display: block;
      margin-bottom: 6px;
      color: #374151;
      font-weight: 500;
    }

    input[type="number"] {
      width: 180px;
      padding: 10px 14px;
      border-radius: 12px;
      border: 1px solid #d1d5db;
      font-size: 15px;
      box-shadow: inset 0 1px 2px rgba(15, 23, 42, 0.05);
    }

    .status-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 16px;
      margin-top: 24px;
    }

    .status-item {
      background: linear-gradient(135deg, #f8fafc, #fff);
      border-radius: 14px;
      padding: 16px;
      border: 1px solid rgba(15, 23, 42, 0.05);
    }

    .status-item .label {
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: #94a3b8;
      margin-bottom: 6px;
    }

    .status-item .value {
      font-size: 17px;
      font-weight: 600;
      color: #1f2937;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 20px;
    }

    th, td {
      padding: 14px 16px;
      text-align: left;
      border-bottom: 1px solid rgba(15, 23, 42, 0.08);
    }

    th {
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: #64748b;
    }

    tbody tr:hover {
      background: rgba(59, 130, 246, 0.08);
    }

    code {
      background: rgba(15, 23, 42, 0.06);
      padding: 4px 8px;
      border-radius: 8px;
      font-size: 13px;
    }

    a.button-link {
      background: #2563eb;
      color: #fff;
      padding: 8px 16px;
      border-radius: 10px;
      text-decoration: none;
      font-weight: 600;
      box-shadow: 0 8px 20px rgba(37, 99, 235, 0.35);
    }

    a.button-link:hover {
      background: #1d4ed8;
    }

    .copy-button {
      background: #10b981;
      color: #fff;
      border: none;
      padding: 8px 14px;
      border-radius: 10px;
      font-weight: 600;
      cursor: pointer;
      box-shadow: 0 8px 20px rgba(16, 185, 129, 0.35);
      transition: transform 0.1s ease, box-shadow 0.2s ease;
    }

    .copy-button:hover {
      transform: translateY(-1px);
      box-shadow: 0 12px 28px rgba(16, 185, 129, 0.45);
    }

    .link-actions {
      display: flex;
      align-items: center;
      gap: 10px;
      margin-bottom: 8px;
    }

    .link-text {
      font-size: 13px;
      color: #4b5563;
      word-break: break-all;
      background: rgba(15, 23, 42, 0.04);
      padding: 6px 10px;
      border-radius: 8px;
    }

    .messages {
      min-height: 20px;
      margin-top: 12px;
    }

    @media (max-width: 640px) {
      .controls {
        flex-direction: column;
        align-items: stretch;
      }

      input[type="number"] {
        width: 100%;
      }

      button {
        width: 100%;
      }
    }
  </style>
</head>
<body>
  <div class=\"container\">
    <div class=\"card\">
      <h1>Aggregator Dashboard</h1>
      <p class=\"muted\">通过可视化界面控制 <code>collect.py</code>，一键生成 Clash 订阅链接。</p>
      <div class=\"controls\">
        <button id=\"run-button\" onclick=\"triggerRun()\">立即执行</button>
        <div>
          <label for=\"interval-input\">自动执行间隔 (分钟)</label>
          <input id=\"interval-input\" type=\"number\" min=\"0\" step=\"1\" value=\"0\" />
        </div>
        <button onclick=\"saveInterval()\">保存定时</button>
        <button class=\"secondary\" onclick=\"disableInterval()\">关闭定时</button>
      </div>
      <div class=\"messages\" id=\"messages\"></div>
      <div class=\"status-grid\" id=\"status-grid\"></div>
    </div>

    <div class=\"card\">
      <h2>生成的订阅链接</h2>
      <table>
        <thead>
          <tr>
            <th>UUID</th>
            <th>创建时间</th>
            <th>文件大小</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody id=\"runs-body\">
          <tr><td colspan=\"4\" class=\"muted\">暂无记录</td></tr>
        </tbody>
      </table>
    </div>
  </div>

<script>
const statusGrid = document.getElementById('status-grid');
const runsBody = document.getElementById('runs-body');
const runButton = document.getElementById('run-button');
const intervalInput = document.getElementById('interval-input');
const messagesEl = document.getElementById('messages');

function formatDate(value) {
  if (!value) { return '—'; }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) { return value; }
  return date.toLocaleString();
}

function formatDuration(seconds) {
  if (!seconds && seconds !== 0) { return '—'; }
  return `${seconds.toFixed(2)} 秒`;
}

function formatSize(bytes) {
  if (!bytes || bytes <= 0) { return '0 B'; }
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let size = bytes;
  let idx = 0;
  while (size >= 1024 && idx < units.length - 1) {
    size /= 1024;
    idx += 1;
  }
  const digits = idx === 0 ? 0 : 2;
  return `${size.toFixed(digits)} ${units[idx]}`;
}

function showMessage(text, isError = false) {
  messagesEl.textContent = text || '';
  messagesEl.style.color = isError ? '#dc2626' : '#6b7280';
  if (text) {
    setTimeout(() => {
      if (messagesEl.textContent === text) {
        messagesEl.textContent = '';
      }
    }, 6000);
  }
}

async function copyToClipboard(text) {
  try {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(text);
    } else {
      const textarea = document.createElement('textarea');
      textarea.value = text;
      textarea.style.position = 'fixed';
      textarea.style.opacity = '0';
      document.body.appendChild(textarea);
      textarea.focus();
      textarea.select();
      document.execCommand('copy');
      document.body.removeChild(textarea);
    }
    showMessage('链接已复制到剪贴板');
  } catch (error) {
    showMessage('无法复制链接，请手动复制', true);
  }
}

runsBody.addEventListener('click', async (event) => {
  const target = event.target;
  if (target && target.classList.contains('copy-button')) {
    event.preventDefault();
    const url = target.getAttribute('data-url');
    if (url) {
      await copyToClipboard(url);
    }
  }
});

async function fetchState() {
  try {
    const response = await fetch('/api/state');
    if (!response.ok) {
      throw new Error('无法获取当前状态');
    }
    const payload = await response.json();
    renderState(payload);
  } catch (error) {
    showMessage(error.message, true);
  }
}

function renderState(state) {
  intervalInput.value = state.interval ?? 0;
  runButton.disabled = Boolean(state.running);

  const nextRun = formatDate(state.next_run);
  const lastRun = formatDate(state.last_run);
  const lastResult = state.last_success === null ? '—' : (state.last_success ? '成功' : '失败');
  const lastMessage = state.last_message || '—';

  statusGrid.innerHTML = `
    <div class="status-item"><div class="label">当前状态</div><div class="value">${state.running ? '运行中' : '空闲'}</div></div>
    <div class="status-item"><div class="label">上次执行</div><div class="value">${lastRun}</div></div>
    <div class="status-item"><div class="label">执行耗时</div><div class="value">${formatDuration(state.last_duration ?? null)}</div></div>
    <div class="status-item"><div class="label">执行结果</div><div class="value">${lastResult}</div></div>
    <div class="status-item"><div class="label">消息</div><div class="value">${lastMessage}</div></div>
    <div class="status-item"><div class="label">下次执行</div><div class="value">${nextRun}</div></div>
  `;

  if (!state.results || state.results.length === 0) {
    runsBody.innerHTML = '<tr><td colspan="4" class="muted">暂无记录</td></tr>';
    return;
  }

  runsBody.innerHTML = state.results.map(item => `
    <tr>
      <td><code>${item.uuid}</code></td>
      <td>${formatDate(item.created_at)}</td>
      <td>${formatSize(item.size)}</td>
      <td>
        <div class="link-actions">
          <a class="button-link" href="${item.absolute_url}" target="_blank" rel="noopener noreferrer">打开</a>
          <button type="button" class="copy-button" data-url="${item.absolute_url}">复制链接</button>
        </div>
        <div class="link-text">${item.absolute_url}</div>
      </td>
    </tr>
  `).join('');
}

async function triggerRun() {
  try {
    runButton.disabled = true;
    const response = await fetch('/api/run', { method: 'POST' });
    const payload = await response.json();
    if (!response.ok || !payload.started) {
      throw new Error(payload.message || '任务启动失败');
    }
    showMessage(payload.message || '已启动新的任务');
    setTimeout(fetchState, 1500);
  } catch (error) {
    showMessage(error.message, true);
  } finally {
    setTimeout(fetchState, 3000);
  }
}

async function saveInterval() {
  try {
    const interval = parseFloat(intervalInput.value || '0');
    const response = await fetch('/api/schedule', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ interval })
    });

    const payload = await response.json();
    if (!response.ok || !payload.success) {
      throw new Error(payload.message || '自动执行间隔设置失败');
    }
    showMessage(payload.message || '定时配置已更新');
    fetchState();
  } catch (error) {
    showMessage(error.message, true);
  }
}

async function disableInterval() {
  intervalInput.value = 0;
  await saveInterval();
}

fetchState();
setInterval(fetchState, 5000);
</script>
</body>
</html>"""


@app.route("/")
def index() -> str:
    return render_template_string(HTML_TEMPLATE)


@app.route("/api/state", methods=["GET"])
def api_state():
    state = manager.get_state()
    results = []
    for item in state.get("results", []):
        data = dict(item)
        data["url"] = url_for("serve_subscription", run_id=item["uuid"])
        data["absolute_url"] = url_for("serve_subscription", run_id=item["uuid"], _external=True)
        results.append(data)
    state["results"] = results
    return jsonify(state)


@app.route("/api/run", methods=["POST"])
def api_run():
    started, message = manager.start_run()
    status_code = 202 if started else 409
    return jsonify({"started": started, "message": message}), status_code


@app.route("/api/schedule", methods=["POST"])
def api_schedule():
    payload = request.get_json(silent=True) or {}
    if "interval" not in payload:
        return jsonify({"success": False, "message": "interval is required"}), 400

    try:
        interval = float(payload.get("interval", 0))
    except (TypeError, ValueError):
        return jsonify({"success": False, "message": "invalid interval value"}), 400

    interval, next_run = manager.update_interval(interval)
    message = (
        "定时任务已关闭"
        if interval <= 0
        else f"已设置每 {interval:.0f} 分钟执行一次，下一次执行时间 {format_datetime(next_run)}"
    )

    return jsonify(
        {
            "success": True,
            "interval": interval,
            "next_run": _iso(next_run),
            "message": message,
        }
    )


def format_datetime(dt: Optional[datetime]) -> str:
    if dt is None:
        return "未安排"
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


@app.route("/subscription/<run_id>", methods=["GET"])
def serve_subscription(run_id: str):
    path, filename = manager.get_file(run_id)
    if not path or not filename:
        abort(404)

    mimetype = mimetypes.guess_type(filename)[0] or "application/octet-stream"
    return send_file(path, mimetype=mimetype, download_name=filename, as_attachment=False, conditional=True)


def create_app() -> Flask:
    """Expose Flask application factory."""
    return app


if __name__ == "__main__":
    host = os.getenv("DASHBOARD_HOST", "0.0.0.0")
    port = _env_int("DASHBOARD_PORT", 8000)
    logger.info("starting dashboard server on %s:%s", host, port)
    app.run(host=host, port=port)
