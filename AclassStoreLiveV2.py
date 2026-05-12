import sys
import asyncio
import html
import os
import uuid
import logging
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# piratetok_live
from piratetok_live import TikTokLiveClient, EventType

# PyQt5
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QTextEdit, QFrame, QGraphicsDropShadowEffect
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QObject
from PyQt5.QtGui import QFont, QTextCursor, QIcon, QColor

# =========================================================
# CONFIG
# =========================================================

SERVER_URL    = "https://api.aclassstore.com"
ACTIVATE_URL  = "https://www.aclassstore.com"
HEARTBEAT_INTERVAL = 15

logging.basicConfig(level=logging.WARNING, force=True)
for name in ("httpx", "httpcore", "TikTokLive", "piratetok_live"):
    logging.getLogger(name).setLevel(logging.CRITICAL)

# =========================================================
# UTILS
# =========================================================

def get_device_id() -> str:
    device_file = os.path.join(os.path.expanduser("~"), ".tiktok_live_device_id")
    try:
        with open(device_file) as f:
            return f.read().strip()
    except FileNotFoundError:
        device_id = str(uuid.uuid4())
        try:
            with open(device_file, "w") as f:
                f.write(device_id)
        except Exception:
            pass
        return device_id

# =========================================================
# MIDDLEWARE CLIENT
# =========================================================

class MiddlewareClient:
    def __init__(self, server_url: str, token: str, username: str):
        self.server_url = server_url.rstrip("/")
        self.username   = username
        self.session    = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })
        retry = Retry(total=3, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _req(self, method, path, json_data=None, params=None, timeout=5) -> bool:
        try:
            r = self.session.request(method, f"{self.server_url}{path}",
                                     json=json_data, params=params, timeout=timeout)
            r.raise_for_status()
            return True
        except requests.RequestException:
            return False

    def register(self):              return self._req("POST",   "/register",   json_data={"username": self.username})
    def push_event(self, t, data):   return self._req("POST",   "/push-event", json_data={"username": self.username, "type": t, "data": data}, timeout=2)
    def heartbeat(self):             return self._req("POST",   "/heartbeat",  json_data={"username": self.username}, timeout=2)
    def stop(self):                  return self._req("DELETE", "/stop",       params={"username": self.username}, timeout=3)
    def close(self):                 self.session.close()

# =========================================================
# WORKER THREAD

class WorkerSignals(QObject):
    status_changed = pyqtSignal(bool, str)
    gift_received  = pyqtSignal(dict)
    error_occurred = pyqtSignal(str)

RETRY_INTERVAL = 30

class TikTokWorker(QThread):
    def __init__(self, license_key: str):
        super().__init__()
        self.license_key = license_key
        self.signals     = WorkerSignals()
        self.is_running  = True
        self.stop_event  = None
        self.loop        = None
        self.client      = None

    def activate_license(self) -> dict:
        url = f"{ACTIVATE_URL.rstrip('/')}/api/whitelist/activate"
        r = requests.post(url, json={
            "licenseKey": self.license_key,
            "deviceId":   get_device_id()
        }, timeout=10)
        data = r.json()
        if not r.ok:
            raise Exception(data.get("error", "License validation failed"))
        return data

    def _attach_handlers(self, client, api):
        """Register event handlers. on() takes a string key matching EventType values."""
        @client.on(EventType.gift)
        def on_gift(evt):
            user      = evt.data.get("user", {})
            gift      = evt.data.get("gift", {})
            nickname  = user.get("nickname", user.get("uniqueId", "?"))
            username  = user.get("uniqueId", "")
            gift_name = gift.get("name", "Unknown")
            diamond   = gift.get("diamondCount", 0)
            repeat    = evt.data.get("repeatCount", 1)
            pic       = (user.get("avatarThumb", {}).get("urlList") or [""])[0]
            api.push_event("gift", {
                "id": str(evt.data.get("id", "0")), "giftId": gift.get("id", 0),
                "giftName": gift_name, "username": username, "nickname": nickname,
                "diamond": diamond, "repeatCount": repeat, "repeatEnd": True,
                "profilePictureUrl": pic,
            })
            self.signals.gift_received.emit({
                "user": nickname or username, "gift_name": gift_name,
                "count": repeat, "diamond": diamond,
            })

        @client.on(EventType.chat)
        def on_chat(evt):
            u = evt.data.get("user", {})
            api.push_event("chat", {
                "username": u.get("uniqueId", ""), "nickname": u.get("nickname", ""),
                "comment": evt.data.get("content", ""),
                "profilePictureUrl": (u.get("avatarThumb", {}).get("urlList") or [""])[0],
            })

        @client.on(EventType.like)
        def on_like(evt):
            u = evt.data.get("user", {})
            api.push_event("like", {
                "username": u.get("uniqueId", ""), "nickname": u.get("nickname", ""),
                "likeCount": evt.data.get("count", 1), "totalLikeCount": evt.data.get("total", 0),
                "profilePictureUrl": (u.get("avatarThumb", {}).get("urlList") or [""])[0],
            })

        @client.on(EventType.follow)
        def on_follow(evt):
            u = evt.data.get("user", {})
            api.push_event("follow", {
                "username": u.get("uniqueId", ""), "nickname": u.get("nickname", ""),
                "profilePictureUrl": (u.get("avatarThumb", {}).get("urlList") or [""])[0],
            })

    def run(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._main())

    async def _main(self):
        self.stop_event = asyncio.Event()
        self.signals.status_changed.emit(False, "Validating license...")

        try:
            activation = await self.loop.run_in_executor(None, self.activate_license)
        except Exception as e:
            self.signals.error_occurred.emit(str(e))
            return

        jwt_token       = activation["token"]
        tiktok_username = (activation.get("tiktokUsername") or
                           activation.get("whitelistedUsername", "")).lstrip("@")

        if not tiktok_username:
            self.signals.error_occurred.emit("No TikTok username found in license")
            return

        api = MiddlewareClient(SERVER_URL, jwt_token, tiktok_username)
        if not api.register():
            self.signals.error_occurred.emit("Failed to register with middleware")
            return

        async def heartbeat_loop():
            while not self.stop_event.is_set():
                await self.loop.run_in_executor(None, api.heartbeat)
                await asyncio.sleep(HEARTBEAT_INTERVAL)

        async def connect_loop():
            from piratetok_live.errors import (
                HostNotOnlineError, PirateTokError,
                TikTokBlockedError, DeviceBlockedError
            )
            while not self.stop_event.is_set():
                # Fresh client per attempt
                self.client = TikTokLiveClient(tiktok_username)
                self._attach_handlers(self.client, api)
                try:
                    self.signals.status_changed.emit(False, f"Connecting to @{tiktok_username}...")
                    # connect() calls check_online() synchronously inside itself —
                    # run the whole thing in executor so it doesn't block the loop
                    await self.loop.run_in_executor(None, self.client.run)
                    # run() returned → stream ended naturally
                    if not self.stop_event.is_set():
                        self.signals.status_changed.emit(False, "Stream ended — reconnecting...")
                        await self._interruptible_sleep(5)

                except HostNotOnlineError:
                    if self.stop_event.is_set():
                        break
                    self.signals.status_changed.emit(False,
                        f"@{tiktok_username} not live — retry in {RETRY_INTERVAL}s")
                    await self._interruptible_sleep(RETRY_INTERVAL)

                except (TikTokBlockedError, DeviceBlockedError) as e:
                    if self.stop_event.is_set():
                        break
                    self.signals.status_changed.emit(False,
                        f"Blocked by TikTok — retry in {RETRY_INTERVAL}s")
                    await self._interruptible_sleep(RETRY_INTERVAL)

                except PirateTokError as e:
                    if self.stop_event.is_set():
                        break
                    self.signals.status_changed.emit(False,
                        f"{type(e).__name__}: {e} — retry in {RETRY_INTERVAL}s")
                    await self._interruptible_sleep(RETRY_INTERVAL)

                except Exception as e:
                    if self.stop_event.is_set():
                        break
                    self.signals.status_changed.emit(False,
                        f"[{type(e).__name__}] {e} — retry in {RETRY_INTERVAL}s")
                    await self._interruptible_sleep(RETRY_INTERVAL)

        # Hook connected/disconnected events to update UI status
        # We do this on the first client before connect_loop creates fresh ones
        # Instead, we listen via a wrapper — status is set inside connect_loop

        hb_task  = asyncio.create_task(heartbeat_loop())
        con_task = asyncio.create_task(connect_loop())

        await self.stop_event.wait()

        hb_task.cancel()
        con_task.cancel()
        # disconnect() is sync — just call it directly
        try:
            if self.client:
                self.client.disconnect()
        except Exception:
            pass
        api.stop()
        api.close()
        self.signals.status_changed.emit(False, "Stopped")

    async def _interruptible_sleep(self, seconds: int):
        for _ in range(seconds):
            if self.stop_event.is_set():
                return
            await asyncio.sleep(1)

    def stop(self):
        self.is_running = False
        if self.loop and self.stop_event:
            asyncio.run_coroutine_threadsafe(self._set_stop(), self.loop)
        # Also call disconnect() directly since it's sync
        try:
            if self.client:
                self.client.disconnect()
        except Exception:
            pass

    async def _set_stop(self):
        self.stop_event.set()


# =========================================================
# MAIN WINDOW
# =========================================================

STYLE = """
QWidget {
    background-color: #0a1520;
    color: #c8d8e8;
    font-family: 'Helvetica Neue', Arial, sans-serif;
    font-size: 13px;
}
QLabel#brandName {
    color: #2e6da4;
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 3px;
}
QLabel#brandTitle {
    font-size: 22px;
    font-weight: 700;
    color: #e8f0f8;
}
QLabel#brandSub {
    color: #2e4a62;
    font-size: 11px;
}
QFrame#pillOffline {
    background-color: #12202e;
    border: 1px solid #1a3045;
    border-radius: 14px;
}
QLabel#dotOffline { color: #8b3a3a; font-size: 9px; }
QLabel#statOffline { color: #3a5470; font-size: 10px; font-weight: 700; letter-spacing: 1.5px; }
QFrame#pillOnline {
    background-color: #0d2010;
    border: 1px solid #1a4020;
    border-radius: 14px;
}
QLabel#dotOnline { color: #2ea84a; font-size: 9px; }
QLabel#statOnline { color: #2ea84a; font-size: 10px; font-weight: 700; letter-spacing: 1.5px; }
QFrame#pillBusy {
    background-color: #1a1a0a;
    border: 1px solid #3a3a10;
    border-radius: 14px;
}
QLabel#dotBusy { color: #b8960a; font-size: 9px; }
QLabel#statBusy { color: #b8960a; font-size: 10px; font-weight: 700; letter-spacing: 1.5px; }
QFrame#sep { border-top: 1px solid #122030; }
QLineEdit {
    background-color: #0e1c2a;
    border: 1px solid #162840;
    border-radius: 8px;
    padding: 0 14px;
    color: #c8d8e8;
    font-size: 13px;
    selection-background-color: #1a4a7a;
}
QLineEdit:focus { border: 1px solid #2563a8; }
QLineEdit:disabled { color: #1e3a55; background-color: #0c1820; }
QPushButton#btnStart {
    background-color: #163a60;
    color: #6aaad8;
    border: 1px solid #1e5080;
    border-radius: 8px;
    padding: 0 15px;
    font-size: 9px;
    font-weight: 700;
    letter-spacing: 1.5px;
}
QPushButton#btnStart:hover { background-color: #1e5080; color: #90c0e8; }
QPushButton#btnStop {
    background-color: #300e0e;
    color: #c04040;
    border: 1px solid #501818;
    border-radius: 8px;
    padding: 0 15px;
    font-size: 9px;
    font-weight: 700;
    letter-spacing: 1.5px;
}
QPushButton#btnStop:hover { background-color: #501818; color: #e06060; }
QLabel#logTitle { color: #1e3a55; font-size: 10px; font-weight: 700; letter-spacing: 2.5px; }
QLabel#giftCount { color: #2a5070; font-size: 10px; }
QTextEdit#logArea {
    background-color: #080f18;
    border: 1px solid #122030;
    border-radius: 10px;
    padding: 10px;
    color: #c8d8e8;
}
QScrollBar:vertical { border: none; background: transparent; width: 4px; }
QScrollBar::handle:vertical { background: #162840; border-radius: 2px; min-height: 24px; }
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }
QLabel#footer { color: #0f2030; font-size: 10px; letter-spacing: 2px; }
"""


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("A Class Store  —  TikTok Live")
        self.setFixedSize(480, 680)
        self.setWindowIcon(QIcon("LogoV.ico"))
        self.worker      = None
        self.total_gifts = 0
        self._build_ui()
        self.setStyleSheet(STYLE)

    # ── UI ─────────────────────────────────────────────
    def _build_ui(self):
        root = QWidget()
        self.setCentralWidget(root)
        lay = QVBoxLayout(root)
        lay.setContentsMargins(24, 22, 24, 18)
        lay.setSpacing(14)

        # Header row
        hdr = QHBoxLayout()
        brand = QVBoxLayout()
        brand.setSpacing(2)
        for obj, text in [("brandName", "A CLASS STORE"),
                           ("brandTitle", "TikTok Live"),
                           ("brandSub", "Gift monitor")]:
            lbl = QLabel(text); lbl.setObjectName(obj); brand.addWidget(lbl)

        # Status pill — we swap objectName to trigger QSS state changes
        self._pill = QFrame()
        self._pill.setObjectName("pillOffline")
        pill_lay = QHBoxLayout(self._pill)
        pill_lay.setContentsMargins(12, 7, 14, 7)
        pill_lay.setSpacing(7)
        self._dot  = QLabel("●"); self._dot.setObjectName("dotOffline")
        self._stat = QLabel("OFFLINE"); self._stat.setObjectName("statOffline")
        pill_lay.addWidget(self._dot)
        pill_lay.addWidget(self._stat)

        hdr.addLayout(brand)
        hdr.addStretch()
        hdr.addWidget(self._pill, alignment=Qt.AlignTop)
        lay.addLayout(hdr)

        sep = QFrame(); sep.setFrameShape(QFrame.HLine); sep.setObjectName("sep")
        lay.addWidget(sep)

        # License input + button
        ctrl = QHBoxLayout(); ctrl.setSpacing(8)
        self._key = QLineEdit()
        self._key.setPlaceholderText("License key")
        self._key.setFixedHeight(40)
        self._key.setEchoMode(QLineEdit.Password)
        self._btn = QPushButton("START")
        self._btn.setObjectName("btnStart")
        self._btn.setFixedHeight(40)
        self._btn.setFixedWidth(80)
        self._btn.setCursor(Qt.PointingHandCursor)
        self._btn.clicked.connect(self._toggle)
        ctrl.addWidget(self._key)
        ctrl.addWidget(self._btn)
        lay.addLayout(ctrl)

        # Log header
        lhdr = QHBoxLayout()
        t = QLabel("GIFT LOG"); t.setObjectName("logTitle")
        self._cnt = QLabel("—"); self._cnt.setObjectName("giftCount")
        lhdr.addWidget(t); lhdr.addStretch(); lhdr.addWidget(self._cnt)
        lay.addLayout(lhdr)

        # Log area
        self._log = QTextEdit()
        self._log.setReadOnly(True)
        self._log.setObjectName("logArea")
        self._log.document().setMaximumBlockCount(600)
        self._log.setHtml(
            '<p style="color:#122030; font-size:12px; text-align:center; margin-top:20px;">'
            'Waiting for gifts...</p>'
        )
        lay.addWidget(self._log)

        # Footer
        foot = QLabel("A CLASS STORE  —  LICENSED SOFTWARE")
        foot.setObjectName("footer"); foot.setAlignment(Qt.AlignCenter)
        lay.addWidget(foot)

    # ── Toggle ─────────────────────────────────────────
    def _toggle(self):
        if self.worker and self.worker.is_running:
            self.worker.stop()
            self.worker = None
            self._set_pill("offline", "OFFLINE")
            self._btn.setText("START"); self._btn.setObjectName("btnStart")
            self._key.setEnabled(True)
            self.setStyleSheet(STYLE)
        else:
            key = self._key.text().strip()
            if not key:
                self._row("License key is required", "ERR"); return
            if self.total_gifts == 0:
                self._log.clear()
            self._btn.setText("STOP"); self._btn.setObjectName("btnStop")
            self._key.setEnabled(False)
            self.setStyleSheet(STYLE)
            self.worker = TikTokWorker(key)
            self.worker.signals.status_changed.connect(self._on_status)
            self.worker.signals.gift_received.connect(self._on_gift)
            self.worker.signals.error_occurred.connect(self._on_error)
            self.worker.start()

    # ── Slots ───────────────────────────────────────────
    def _on_status(self, connected: bool, message: str):
        msg_lc = message.lower()
        if connected:
            self._set_pill("online", "LIVE")
        elif any(k in msg_lc for k in ("connect", "validat", "retry")):
            self._set_pill("busy", "WAIT")
        else:
            self._set_pill("offline", "OFFLINE")
        self._row(message, "SYS")

    def _on_error(self, msg: str):
        self._set_pill("offline", "ERROR")
        self._row(msg, "ERR")
        if self.worker:
            self.worker.stop(); self.worker = None
            self._btn.setText("START"); self._btn.setObjectName("btnStart")
            self._key.setEnabled(True)
            self.setStyleSheet(STYLE)

    def _on_gift(self, data: dict):
        self.total_gifts += 1
        self._cnt.setText(f"{self.total_gifts} gift{'s' if self.total_gifts > 1 else ''}")

        user    = html.escape(data.get("user", "?"))
        gift    = html.escape(data.get("gift_name", ""))
        count   = data.get("count", 1)
        diamond = data.get("diamond", 0)

        body = (
            f'<b style="color:#d0e4f4;">{user}</b>'
            f'<span style="color:#2a4a62;">  sent  </span>'
            f'<b style="color:#5a9ed0;">{gift}</b>'
            f'<span style="color:#2a4a62;">  x{count}  </span>'
            f'<b style="color:#c8a030;">{diamond} dia</b>'
        )
        self._row(body, "GIFT")

    # ── Helpers ─────────────────────────────────────────
    def _set_pill(self, state: str, label: str):
        """state: 'online' | 'busy' | 'offline'"""
        frames = {"online": "pillOnline", "busy": "pillBusy", "offline": "pillOffline"}
        dots   = {"online": "dotOnline",  "busy": "dotBusy",  "offline": "dotOffline"}
        stats  = {"online": "statOnline", "busy": "statBusy", "offline": "statOffline"}

        self._pill.setObjectName(frames.get(state, "pillOffline"))
        self._dot.setObjectName(dots.get(state, "dotOffline"))
        self._stat.setObjectName(stats.get(state, "statOffline"))
        self._stat.setText(label)

        # Force QSS repaint on all three widgets
        for w in (self._pill, self._dot, self._stat):
            w.style().unpolish(w)
            w.style().polish(w)
            w.update()

    def _row(self, content_html: str, tag: str):
        ts = datetime.now().strftime("%H:%M:%S")
        border_col = {"GIFT": "#162840", "ERR": "#3a1010", "SYS": "#0e1c28"}.get(tag, "#0e1c28")
        row = (
            f'<div style="padding:8px 4px; border-bottom:1px solid {border_col};">'
            f'<span style="color:#1a3a55; font-size:10px;">{ts}</span>'
            f'&nbsp;&nbsp;'
            f'<span style="color:#1e3a55; font-size:10px; font-weight:700; letter-spacing:1px;">{tag}</span>'
            f'&nbsp;&nbsp;&nbsp;'
            f'<span style="font-size:13px;">{content_html}</span>'
            f'</div>'
        )
        self._log.append(row)
        self._log.moveCursor(QTextCursor.End)

    def closeEvent(self, event):
        if self.worker:
            self.worker.stop()
        event.accept()


# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":
    app = QApplication(sys.argv)
    # Use system default font — avoids missing-font warning on macOS/Linux
    font = app.font()
    font.setPointSize(10)
    app.setFont(font)

    win = MainWindow()
    win.show()
    sys.exit(app.exec_())
