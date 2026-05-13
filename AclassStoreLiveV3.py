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

# TikTokLive
from TikTokLive import TikTokLiveClient
from TikTokLive.events import (
    ConnectEvent, DisconnectEvent, LiveEndEvent,
    GiftEvent, CommentEvent, LikeEvent, FollowEvent
)

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

SERVER_URL         = "https://api.aclassstore.com"
ACTIVATE_URL       = "https://www.aclassstore.com"
HEARTBEAT_INTERVAL = 15

logging.basicConfig(level=logging.WARNING, force=True)
for _name in ("httpx", "httpcore", "TikTokLive"):
    logging.getLogger(_name).setLevel(logging.CRITICAL)

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

def _get_avatar(user) -> str:
    """Safely extract first avatar URL from any user object."""
    try:
        thumb = getattr(user, "avatar_thumb", None)
        if thumb:
            urls = getattr(thumb, "m_urls", None) or getattr(thumb, "url_list", None)
            if urls:
                return urls[0]
    except Exception:
        pass
    return ""

def _get_nick(user) -> str:
    """nick_name in v6.x, nickname in older versions."""
    return (getattr(user, "nick_name", None)
            or getattr(user, "nickname", None)
            or getattr(user, "username", None)
            or "")

def _get_username(user) -> str:
    """username in v6.x, unique_id in older versions."""
    return (getattr(user, "username", None)
            or getattr(user, "unique_id", None)
            or getattr(user, "uniqueId", None)
            or "")

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

    def register(self):            return self._req("POST",   "/register",   json_data={"username": self.username})
    def push_event(self, t, data): return self._req("POST",   "/push-event", json_data={"username": self.username, "type": t, "data": data}, timeout=2)
    def heartbeat(self):           return self._req("POST",   "/heartbeat",  json_data={"username": self.username}, timeout=2)
    def stop(self):                return self._req("DELETE", "/stop",       params={"username": self.username}, timeout=3)
    def close(self):               self.session.close()

# =========================================================
# WORKER THREAD
# =========================================================

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

    def run(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._main())

# แก้ไขเฉพาะส่วนใน TikTokWorker ภายในไฟล์ AclassStoreLive.py

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
            try:
                while not self.stop_event.is_set():
                    await self.loop.run_in_executor(None, api.heartbeat)
                    await asyncio.sleep(HEARTBEAT_INTERVAL)
            except asyncio.CancelledError:
                pass

        async def connect_loop():
            while not self.stop_event.is_set():
                self.client = TikTokLiveClient(unique_id=tiktok_username)

                @self.client.on(ConnectEvent)
                async def on_connect(_):
                    self.signals.status_changed.emit(True, f"Live  @{tiktok_username}")
                    api.push_event("status", {"connected": True})

                @self.client.on(DisconnectEvent)
                async def on_disconnect(_):
                    pass

                @self.client.on(LiveEndEvent)
                async def on_live_end(_):
                    self.stop_event.set()

                @self.client.on(GiftEvent)
                async def on_gift(event: GiftEvent):
                    # --- Real-time Logic ---
                    # 1. ถ้ามีมูลค่า (Diamond > 0) ให้ส่งข้อมูลทันทีทุกครั้งที่จำนวนขยับ (x1, x2, x3...)
                    # 2. ถ้าเป็นของขวัญฟรี ให้รอจนส่งจบชุด (repeat_end) เพื่อประหยัด API
                    if event.m_gift.diamond_count > 0:
                        pass 
                    elif not event.repeat_end:
                        return
                    # -----------------------

                    user      = event.from_user
                    gift      = event.m_gift
                    
                    # ข้อมูลผู้ส่ง
                    nick      = _get_nick(user)
                    uname     = _get_username(user)
                    pic       = _get_avatar(user)
                    
                    # ข้อมูลของขวัญและราคา
                    gift_id   = getattr(gift, "id", 0)
                    gift_name = getattr(gift, "name", "Unknown")
                    
                    # --- ส่วนการคำนวณราคา ---
                    diamond_per_unit = getattr(gift, "diamond_count", 0) # ราคาต่อ 1 ชิ้น
                    repeat_count     = getattr(event, "repeat_count", 1) # จำนวนปัจจุบัน (x1, x2, x3...)
                    total_value      = diamond_per_unit * repeat_count  # ราคารวม (แบบคูณ *)
                    # -----------------------
                    
                    is_streak_end = event.repeat_end # บอกเกมว่าจบชุดนี้หรือยัง

                    # 1. ส่งข้อมูลครบชุดไปที่ Middleware API (เพื่อส่งต่อเข้าเกม Roblox)
                    api.push_event("gift", {
                        "id":                str(getattr(event, "id", "0")),
                        "giftId":            gift_id,
                        "giftName":          gift_name,
                        "username":          uname,
                        "nickname":          nick,
                        "diamond":           diamond_per_unit, # ราคาต่อชิ้น
                        "repeatCount":       repeat_count,     # จำนวนชิ้น ณ ขณะนั้น
                        "totalValue":        total_value,      # ราคารวมทั้งหมด (คูณให้แล้ว)
                        "repeatEnd":         is_streak_end,
                        "profilePictureUrl": pic,
                        "timestamp":         datetime.now().isoformat()
                    })

                    # 2. ส่งสัญญาณไปแสดงผลที่หน้าจอ Log (UI) ของโปรแกรม
                    self.signals.gift_received.emit({
                        "user":      nick or uname,
                        "gift_name": gift_name,
                        "count":     repeat_count,
                        "diamond":   diamond_per_unit,
                        "total":     total_value
                    })

                @self.client.on(CommentEvent)
                async def on_comment(event: CommentEvent):
                    user = event.user_info
                    api.push_event("chat", {
                        "username":         _get_username(user),
                        "nickname":         _get_nick(user),
                        "comment":          getattr(event, "content", ""),
                        "profilePictureUrl": _get_avatar(user),
                    })

                @self.client.on(LikeEvent)
                async def on_like(event: LikeEvent):
                    user = event.user
                    api.push_event("like", {
                        "username":         _get_username(user),
                        "nickname":         _get_nick(user),
                        "likeCount":        getattr(event, "count", 1),
                        "totalLikeCount":   getattr(event, "total", 0),
                        "profilePictureUrl": _get_avatar(user),
                    })

                @self.client.on(FollowEvent)
                async def on_follow(event: FollowEvent):
                    user = event.user
                    api.push_event("follow", {
                        "username":         _get_username(user),
                        "nickname":         _get_nick(user),
                        "profilePictureUrl": _get_avatar(user),
                    })

                try:
                    self.signals.status_changed.emit(False, f"Connecting to @{tiktok_username}...")
                    await self.client.connect()
                    if not self.stop_event.is_set():
                        self.signals.status_changed.emit(False,
                            f"@{tiktok_username} not live — retry in {RETRY_INTERVAL}s")
                        await self._interruptible_sleep(RETRY_INTERVAL)

                except Exception as e:
                    if self.stop_event.is_set():
                        break
                    err = f"[{type(e).__name__}] {e}" if str(e) else type(e).__name__
                    self.signals.status_changed.emit(False,
                        f"{err} — retry in {RETRY_INTERVAL}s")
                    await self._interruptible_sleep(RETRY_INTERVAL)

        hb_task  = asyncio.create_task(heartbeat_loop())
        con_task = asyncio.create_task(connect_loop())

        await self.stop_event.wait()

        # แก้ไข: ยกเลิก task อย่างปลอดภัยเพื่อไม่ให้เกิด CancelledError จนโปรแกรมค้าง
        hb_task.cancel()
        con_task.cancel()
        
        try:
            if self.client:
                # ใช้ stop() แทนการรอ disconnect() ที่อาจค้างจาก task ที่ถูกยกเลิก
                self.client.stop() 
        except:
            pass

        try:
            await asyncio.gather(hb_task, con_task, return_exceptions=True)
        except:
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

    async def _set_stop(self):
        self.stop_event.set()

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
    border: 1px solid #1a3045;
    border-radius: 14px;
}
QLabel#dotOffline { color: #8b3a3a; font-size: 9px; }
QLabel#statOffline { color: #3a5470; font-size: 10px; font-weight: 700; letter-spacing: 1.5px; }
QFrame#pillOnline {
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
            f'<b style="color:#c8a030;">{diamond} diamond</b>'
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
