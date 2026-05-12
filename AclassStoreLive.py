import sys
import asyncio
import json
import logging
import uuid
import os
import html
from datetime import datetime
from typing import Any, Dict, Optional
from PyQt5.QtGui import QFont, QColor, QTextCursor, QIcon

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
    QLabel, QLineEdit, QPushButton, QTextEdit, QFrame
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QObject
from PyQt5.QtGui import QFont, QColor, QTextCursor

# =========================================================
# CONFIGURATION & UTILS (จากต้นฉบับ)
# =========================================================

SERVER_URL = "https://api.aclassstore.com"
ACTIVATE_URL = "https://www.aclassstore.com"
HEARTBEAT_INTERVAL = 15

logging.basicConfig(level=logging.WARNING, force=True)
logging.getLogger("httpx").setLevel(logging.CRITICAL)
logging.getLogger("httpcore").setLevel(logging.CRITICAL)
logging.getLogger("TikTokLive").setLevel(logging.CRITICAL)

def get_device_id() -> str:
    home_dir = os.path.expanduser("~")
    device_file = os.path.join(home_dir, ".tiktok_live_device_id")
    try:
        with open(device_file, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        device_id = str(uuid.uuid4())
        try:
            with open(device_file, "w") as f:
                f.write(device_id)
        except Exception:
            pass
        return device_id

def extract_user_info(event):
    user = getattr(event, "user_info", None)
    if not user: return {"username": "", "nickname": "", "profilePictureUrl": ""}
    username = getattr(user, "unique_id", None) or getattr(user, "uniqueId", "") or ""
    nickname = getattr(user, "nickname", None) or getattr(user, "nickName", "") or ""
    avatar = ""
    try:
        avatar_thumb = getattr(user, "avatar_thumb", None)
        if avatar_thumb and getattr(avatar_thumb, "url_list", None) and avatar_thumb.url_list:
            avatar = avatar_thumb.url_list[0]
    except Exception: pass
    return {"username": username, "nickname": nickname, "profilePictureUrl": avatar}

def safe_get_avatar_url(user) -> str:
    try:
        avatar = getattr(user, "avatar_thumb", None)
        if avatar and getattr(avatar, "url_list", None) and avatar.url_list:
            return avatar.url_list[0]
    except Exception: pass
    return ""

# =========================================================
# MIDDLEWARE CLIENT
# =========================================================

class MiddlewareClient:
    def __init__(self, server_url: str, token: str, username: str):
        self.server_url = server_url.rstrip("/")
        self.username = username
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })
        retry = Retry(total=3, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _request(self, method: str, path: str, json_data=None, params=None, timeout=5) -> bool:
        try:
            response = self.session.request(method, f"{self.server_url}{path}", json=json_data, params=params, timeout=timeout)
            response.raise_for_status()
            return True
        except requests.RequestException:
            return False

    def register(self): return self._request("POST", "/register", json_data={"username": self.username})
    def push_event(self, event_type, data): return self._request("POST", "/push-event", json_data={"username": self.username, "type": event_type, "data": data}, timeout=2)
    def heartbeat(self): return self._request("POST", "/heartbeat", json_data={"username": self.username}, timeout=2)
    def stop(self): return self._request("DELETE", "/stop", params={"username": self.username}, timeout=3)
    def close(self): self.session.close()

# =========================================================
# WORKER THREAD (Backend Logic)
# =========================================================

class WorkerSignals(QObject):
    # ส่งข้อมูลกลับไปให้ UI
    status_changed = pyqtSignal(bool, str) # connected, message
    event_received = pyqtSignal(dict)      # log data
    error_occurred = pyqtSignal(str)       # error message

class TikTokWorker(QThread):
    def __init__(self, license_key: str):
        super().__init__()
        self.license_key = license_key
        self.signals = WorkerSignals()
        self.is_running = True
        self.stop_event = None
        self.loop = None
        self.client = None

    def activate_license(self) -> dict:
        url = f"{ACTIVATE_URL.rstrip('/')}/api/whitelist/activate"
        response = requests.post(url, json={"licenseKey": self.license_key, "deviceId": get_device_id()}, timeout=10)
        data = response.json()
        if not response.ok:
            raise Exception(data.get("error", "License validation failed"))
        return data

    def run(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self.main())

    async def main(self):
        self.stop_event = asyncio.Event()
        self.signals.status_changed.emit(False, "Validating license key...")

        try:
            # ใช้ loop.run_in_executor เพื่อไม่ให้ blocking requests ดึง loop ค้าง
            activation = await self.loop.run_in_executor(None, self.activate_license)
        except Exception as e:
            self.signals.error_occurred.emit(str(e))
            return

        jwt_token = activation["token"]
        tiktok_username = activation.get("tiktokUsername") or activation.get("whitelistedUsername", "")

        if not tiktok_username:
            self.signals.error_occurred.emit("No TikTok username found in license")
            return

        target_username = tiktok_username.lstrip("@")
        self.signals.status_changed.emit(False, f"Connecting to @{target_username}...")

        api = MiddlewareClient(SERVER_URL, jwt_token, target_username)
        if not api.register():
            self.signals.error_occurred.emit("Failed to register with middleware")
            return

        self.client = TikTokLiveClient(unique_id=target_username)

        # Event Handlers
        @self.client.on(ConnectEvent)
        async def on_connect(_: ConnectEvent):
            self.signals.status_changed.emit(True, f"Live: @{target_username}")
            api.push_event("status", {"connected": True, "message": f"Connected: @{target_username}"})

        @self.client.on(DisconnectEvent)
        async def on_disconnect(_: DisconnectEvent):
            self.signals.status_changed.emit(False, "Disconnected")

        @self.client.on(LiveEndEvent)
        async def on_live_end(_: LiveEndEvent):
            self.signals.status_changed.emit(False, "Live ended")
            self.stop_event.set()

        @self.client.on(CommentEvent)
        async def on_comment(event: CommentEvent):
            user = extract_user_info(event)
            comment = getattr(event, "comment", "")
            api.push_event("chat", {"username": user["username"], "nickname": user["nickname"], "comment": comment, "profilePictureUrl": user["profilePictureUrl"]})
            self.signals.event_received.emit({"type": "chat", "user": user["username"], "comment": comment})

        @self.client.on(LikeEvent)
        async def on_like(event: LikeEvent):
            user = extract_user_info(event)
            count = getattr(event, "count", 1)
            api.push_event("like", {"username": user["username"], "nickname": user["nickname"], "likeCount": count, "totalLikeCount": getattr(event, "total", 0), "profilePictureUrl": user["profilePictureUrl"]})
            self.signals.event_received.emit({"type": "like", "user": user["username"], "count": count})

        @self.client.on(FollowEvent)
        async def on_follow(event: FollowEvent):
            api.push_event("follow", {"username": event.user.unique_id, "nickname": event.user.nickname, "profilePictureUrl": safe_get_avatar_url(event.user)})

        @self.client.on(GiftEvent)
        async def on_gift(event: GiftEvent):
            if getattr(event.gift, "streaking", False): return
            user = extract_user_info(event)
            diamond = getattr(event.gift, "diamond_count", None) or getattr(event.gift, "diamondCount", 0)
            repeat_count = getattr(event, "repeat_count", 1)
            gift_name = getattr(event.gift, "name", "Unknown")

            api.push_event("gift", {
                "id": str(getattr(event, "id", "0")), "giftId": getattr(event.gift, "id", 0),
                "giftName": gift_name, "username": user["username"], "nickname": user["nickname"],
                "diamond": diamond, "repeatCount": repeat_count, "repeatEnd": True,
                "profilePictureUrl": user["profilePictureUrl"]
            })
            self.signals.event_received.emit({"type": "gift", "user": user["username"], "gift_name": gift_name, "count": repeat_count, "diamond": diamond})

        async def heartbeat_loop():
            while not self.stop_event.is_set():
                await self.loop.run_in_executor(None, api.heartbeat)
                await asyncio.sleep(HEARTBEAT_INTERVAL)

        heartbeat_task = asyncio.create_task(heartbeat_loop())
        client_task = asyncio.create_task(self.client.start())

        await self.stop_event.wait()

        heartbeat_task.cancel()
        client_task.cancel()
        try:
            if hasattr(self.client, "disconnect"): await self.client.disconnect()
            elif hasattr(self.client, "stop"): await self.client.stop()
        except Exception: pass
        api.stop()
        api.close()
        self.signals.status_changed.emit(False, "Stopped")

    def stop(self):
        self.is_running = False
        if self.loop and self.stop_event:
            asyncio.run_coroutine_threadsafe(self._set_stop_event(), self.loop)
            
    async def _set_stop_event(self):
        self.stop_event.set()

# =========================================================
# MAIN GUI (PyQt5)
# =========================================================

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("A Class Store — TikTok Live")
        self.setFixedSize(450, 850) # ขนาดเดียวกับ Electron
        self.setWindowIcon(QIcon("LogoV.ico"))
        self.worker = None
        self.total_events = 0
        
        self.setup_ui()
        self.apply_styles()

    def setup_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(15)

        # --- Header ---
        header_layout = QHBoxLayout()
        
        brand_layout = QVBoxLayout()
        brand_layout.setSpacing(2)
        lbl_brand = QLabel("A CLASS STORE")
        lbl_brand.setObjectName("brandName")
        lbl_title = QLabel("TikTok Live System")
        lbl_title.setObjectName("brandTitle")
        lbl_sub = QLabel("Real-time event stream")
        lbl_sub.setObjectName("brandSub")
        brand_layout.addWidget(lbl_brand)
        brand_layout.addWidget(lbl_title)
        brand_layout.addWidget(lbl_sub)
        
        self.status_container = QFrame()
        self.status_container.setObjectName("statusPill")
        status_layout = QHBoxLayout(self.status_container)
        status_layout.setContentsMargins(14, 7, 14, 7)
        self.status_dot = QLabel("●")
        self.status_dot.setObjectName("dotOffline")
        self.status_label = QLabel("OFFLINE")
        self.status_label.setObjectName("statLabel")
        status_layout.addWidget(self.status_dot)
        status_layout.addWidget(self.status_label)
        
        header_layout.addLayout(brand_layout)
        header_layout.addStretch()
        header_layout.addWidget(self.status_container, alignment=Qt.AlignTop)
        
        # --- Controls ---
        control_layout = QHBoxLayout()
        self.input_key = QLineEdit()
        self.input_key.setPlaceholderText("Enter license key")
        self.input_key.setFixedHeight(42)
        self.btn_toggle = QPushButton("START")
        self.btn_toggle.setObjectName("btnStart")
        self.btn_toggle.setFixedHeight(42)
        self.btn_toggle.setCursor(Qt.PointingHandCursor)
        self.btn_toggle.clicked.connect(self.toggle_monitor)
        
        control_layout.addWidget(self.input_key)
        control_layout.addWidget(self.btn_toggle)

        # --- Log Info ---
        log_info_layout = QHBoxLayout()
        lbl_log_title = QLabel("EVENT LOG")
        lbl_log_title.setObjectName("logTitle")
        self.lbl_count = QLabel("0 events")
        self.lbl_count.setObjectName("eventCount")
        log_info_layout.addWidget(lbl_log_title)
        log_info_layout.addStretch()
        log_info_layout.addWidget(self.lbl_count)

        # --- Log Area ---
        self.log_area = QTextEdit()
        self.log_area.setReadOnly(True)
        self.log_area.setObjectName("logArea")
        self.log_area.document().setMaximumBlockCount(300) # จำกัดบรรทัดเพื่อกัน RAM เต็ม
        self.log_area.setHtml('<div style="text-align:center; color:#2d4560; margin-top:20px;">Waiting for events...</div>')

        # --- Footer ---
        lbl_footer = QLabel("A CLASS STORE — LICENSED SOFTWARE")
        lbl_footer.setObjectName("footer")
        lbl_footer.setAlignment(Qt.AlignCenter)

        # Add to main layout
        main_layout.addLayout(header_layout)
        
        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        line.setObjectName("separator")
        main_layout.addWidget(line)
        
        main_layout.addLayout(control_layout)
        main_layout.addLayout(log_info_layout)
        main_layout.addWidget(self.log_area)
        main_layout.addWidget(lbl_footer)

    def apply_styles(self):
        # ถอด CSS จาก index.html มาเป็น QSS
        self.setStyleSheet("""
            QWidget {
                background-color: #07101a;
                color: #eef3f9;
                font-family: 'Segoe UI', Inter, sans-serif;
            }
            QLabel#brandName { color: #5b93cc; font-size: 11px; font-weight: bold; letter-spacing: 1px; }
            QLabel#brandTitle { font-size: 18px; font-weight: bold; }
            QLabel#brandSub { color: #7a9bb8; font-size: 11px; }
            
            QFrame#statusPill {
                background-color: rgba(13, 30, 46, 150);
                border: 1px solid rgba(66, 122, 181, 45);
                border-radius: 15px;
            }
            QLabel#dotOffline { color: #e05c5c; font-size: 14px; }
            QLabel#dotOnline { color: #3ecf8e; font-size: 14px; }
            QLabel#statLabel { color: #7a9bb8; font-size: 10px; font-weight: bold; }
            
            QLineEdit {
                background-color: rgba(13, 30, 46, 180);
                border: 1px solid rgba(66, 122, 181, 45);
                border-radius: 10px;
                padding: 0 15px;
                font-size: 13px;
            }
            QLineEdit:focus { border: 1px solid #427ab5; }
            QLineEdit:disabled { color: #7a9bb8; }
            
            QPushButton#btnStart {
                background-color: #427ab5;
                color: white;
                border-radius: 10px;
                padding: 0 24px;
                font-size: 12px;
                font-weight: bold;
            }
            QPushButton#btnStart:hover { background-color: #5b93cc; }
            
            QPushButton#btnStop {
                background-color: rgba(224, 92, 92, 30);
                color: #e05c5c;
                border: 1px solid rgba(224, 92, 92, 60);
                border-radius: 10px;
                padding: 0 24px;
                font-size: 12px;
                font-weight: bold;
            }
            QPushButton#btnStop:hover { background-color: rgba(224, 92, 92, 50); }
            
            QLabel#logTitle { color: #7a9bb8; font-size: 11px; font-weight: bold; }
            QLabel#eventCount { color: #3a5470; font-size: 11px; }
            
            QTextEdit#logArea {
                background-color: rgba(7, 16, 26, 120);
                border: 1px solid rgba(66, 122, 181, 45);
                border-radius: 12px;
                padding: 10px;
            }
            QScrollBar:vertical {
                border: none;
                background: transparent;
                width: 6px;
                border-radius: 3px;
            }
            QScrollBar::handle:vertical {
                background: rgba(66, 122, 181, 50);
                border-radius: 3px;
            }
            
            QFrame#separator { border-top: 1px solid rgba(66, 122, 181, 45); }
            QLabel#footer { color: #2d4560; font-size: 10px; letter-spacing: 1px; }
        """)

    def toggle_monitor(self):
        if self.worker is not None and self.worker.is_running:
            # Stop Process
            self.worker.stop()
            self.worker = None
            self.update_status(False, "Stopped")
            self.btn_toggle.setText("START")
            self.btn_toggle.setObjectName("btnStart")
            self.input_key.setEnabled(True)
            self.setStyleSheet(self.styleSheet()) # refresh QSS
        else:
            # Start Process
            key = self.input_key.text().strip()
            if not key:
                self.add_log_html('<span style="color:#e05c5c; font-weight:bold;">Error: License key is required</span>', "ERROR")
                return
            
            if self.total_events == 0:
                self.log_area.clear()

            self.btn_toggle.setText("STOP")
            self.btn_toggle.setObjectName("btnStop")
            self.input_key.setEnabled(False)
            self.setStyleSheet(self.styleSheet())

            self.worker = TikTokWorker(key)
            self.worker.signals.status_changed.connect(self.update_status)
            self.worker.signals.event_received.connect(self.handle_event)
            self.worker.signals.error_occurred.connect(self.handle_error)
            self.worker.start()

    def update_status(self, connected: bool, message: str):
        if connected:
            self.status_dot.setObjectName("dotOnline")
            self.status_label.setText("LIVE")
        else:
            self.status_dot.setObjectName("dotOffline")
            self.status_label.setText("OFFLINE" if message in ["Stopped", "Disconnected"] else "CONNECTING")
        
        self.status_container.setStyleSheet(self.status_container.styleSheet()) # Refresh styles
        self.add_log_html(f'<span style="color:#7a9bb8;">{html.escape(message)}</span>', "SYS")

    def handle_error(self, message: str):
        self.update_status(False, "Error")
        self.add_log_html(f'<span style="color:#e05c5c;">{html.escape(message)}</span>', "ERROR")
        if self.worker:
            self.worker.stop()
            self.worker = None
            self.btn_toggle.setText("START")
            self.btn_toggle.setObjectName("btnStart")
            self.input_key.setEnabled(True)
            self.setStyleSheet(self.styleSheet())

    def handle_event(self, data: dict):
        t = data.get("type")
        u = html.escape(data.get("user", ""))
        
        if t == "chat":
            msg = html.escape(data.get("comment", ""))
            content = f'<span style="color:#eef3f9; font-weight:bold;">{u}</span> <span style="color:#7a9bb8;">{msg}</span>'
            self.add_log_html(content, "CHAT")
        elif t == "like":
            count = data.get("count", 1)
            content = f'<span style="color:#eef3f9; font-weight:bold;">{u}</span> <span style="color:#7a9bb8;">liked x{count}</span>'
            self.add_log_html(content, "LIKE")
        elif t == "gift":
            gift = html.escape(data.get("gift_name", ""))
            count = data.get("count", 1)
            diamond = data.get("diamond", 0)
            content = f'<span style="color:#eef3f9; font-weight:bold;">{u}</span> <span style="color:#7a9bb8;">sent</span> <span style="color:#5b93cc; font-weight:bold;">{gift}</span> <span style="color:#7a9bb8;">x{count}</span> <span style="color:#f0c060; font-weight:bold;">{diamond} diamonds</span>'
            self.add_log_html(content, "GIFT")

    def add_log_html(self, content_html: str, tag: str):
        self.total_events += 1
        self.lbl_count.setText(f"{self.total_events} event{'s' if self.total_events > 1 else ''}")
        
        ts = datetime.now().strftime("%H:%M:%S")
        
        # Color palettes สำหรับ Tag เลียนแบบ CSS
        tag_styles = {
            "GIFT": "background-color:rgba(66,122,181,0.15); color:#5b93cc; border:1px solid rgba(66,122,181,0.2);",
            "LIKE": "background-color:rgba(224,92,92,0.12); color:#e07878; border:1px solid rgba(224,92,92,0.2);",
            "CHAT": "background-color:rgba(62,207,142,0.1); color:#3ecf8e; border:1px solid rgba(62,207,142,0.2);",
            "SYS":  "background-color:rgba(122,155,184,0.1); color:#7a9bb8; border:1px solid rgba(122,155,184,0.15);",
            "ERROR":"background-color:rgba(224,92,92,0.15); color:#e05c5c; border:1px solid rgba(224,92,92,0.25);"
        }
        
        t_style = tag_styles.get(tag, tag_styles["SYS"])
        
        # โครงสร้าง HTML สำหรับ 1 แถวของ Log
        row_html = f"""
        <div style="margin-bottom: 8px;">
            <span style="color:#2d4560; font-size:10px; font-weight:bold;">{ts}</span> &nbsp;
            <span style="{t_style} font-size:9px; font-weight:bold; padding:2px 6px;">&nbsp;{tag}&nbsp;</span> &nbsp;
            <span style="font-size:13px;">{content_html}</span>
        </div>
        """
        
        self.log_area.append(row_html)
        # เลื่อน Scroll ลงล่างสุดอัตโนมัติ
        self.log_area.moveCursor(QTextCursor.End)

    def closeEvent(self, event):
        # สั่งปิด Worker คลีนๆ ตอนกดปิดโปรแกรม
        if self.worker:
            self.worker.stop()
        event.accept()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    # เพิ่ม Font ให้รองรับ
    font = QFont("Inter", 10)
    font.setStyleHint(QFont.SansSerif)
    app.setFont(font)
    
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())