import asyncio
import json
import logging
import signal
import ssl
import sys
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

import requests
from TikTokLive.events import FollowEvent
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from TikTokLive import TikTokLiveClient
from TikTokLive.events import (
    ConnectEvent,
    DisconnectEvent,
    LiveEndEvent,
    GiftEvent,
    CommentEvent,
    LikeEvent,
)

# =========================================================
# CONFIGURATION
# =========================================================

SERVER_URL = "https://api.aclassstore.com"         # Middleware (Node.js)
ACTIVATE_URL = "https://www.aclassstore.com"    # Next.js server ของคุณ

HEARTBEAT_INTERVAL = 15
REQUEST_TIMEOUT = 5

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s | %(levelname)s | %(message)s",
    force=True,
)

logger = logging.getLogger("TikTokBridge")
logger.setLevel(logging.WARNING)

logging.getLogger("httpx").setLevel(logging.CRITICAL)
logging.getLogger("httpcore").setLevel(logging.CRITICAL)
logging.getLogger("TikTokLive").setLevel(logging.CRITICAL)

# =========================================================
# SSL FIX
# =========================================================

try:
    ssl._create_default_https_context = ssl._create_unverified_context
except Exception:
    pass

# =========================================================
# GLOBALS
# =========================================================

stop_event = asyncio.Event()
TARGET_USERNAME = ""

# =========================================================
# UTILITIES
# =========================================================

import os

def get_device_id() -> str:
    """
    สร้าง/อ่าน Device ID จากไฟล์ local เพื่อผูกกับเครื่อง
    """
    # เก็บไว้ใน Home Directory เพื่อให้แน่ใจว่ามีสิทธิ์เขียนไฟล์ทั้งใน dev และ production
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
        except Exception as e:
            # fallback กรณีเขียนไม่ได้จริงๆ
            logger.error(f"Cannot write device_id to {device_file}: {e}")
        return device_id


def activate_license(license_key: str) -> Dict[str, Any]:
    """
    เรียก /api/desktop/activate เพื่อตรวจสอบ License Key
    คืนค่า { token, tiktokUsername, ... } หรือ raise Exception
    """
    device_id = get_device_id()
    url = f"{ACTIVATE_URL.rstrip('/')}/api/whitelist/activate"

    try:
        response = requests.post(
            url,
            json={
                "licenseKey": license_key,
                "deviceId": device_id,
            },
            timeout=10,
        )
        data = response.json()

        if not response.ok:
            error_msg = data.get("error", "License validation failed")
            raise Exception(error_msg)

        return data

    except requests.RequestException as e:
        raise Exception(f"Cannot connect to activation server: {e}")


def extract_user_info(event):
    user = getattr(event, "user_info", None)

    if not user:
        return {"username": "", "nickname": "", "profilePictureUrl": ""}

    username = (
        getattr(user, "unique_id", None)
        or getattr(user, "uniqueId", None)
        or ""
    )

    nickname = (
        getattr(user, "nickname", None)
        or getattr(user, "nick_name", None)
        or getattr(user, "nickName", None)
        or ""
    )

    avatar = ""
    try:
        avatar_thumb = getattr(user, "avatar_thumb", None)
        if avatar_thumb and getattr(avatar_thumb, "url_list", None):
            if avatar_thumb.url_list:
                avatar = avatar_thumb.url_list[0]
    except Exception:
        pass

    return {"username": username, "nickname": nickname, "profilePictureUrl": avatar}


def emit(event_type: str, data: Dict[str, Any]) -> None:
    payload = {
        "type": event_type,
        "timestamp": datetime.now().strftime("%H:%M:%S"),
        **data,
    }
    print(json.dumps(payload, ensure_ascii=False), flush=True)


def safe_get_avatar_url(user) -> str:
    try:
        avatar = getattr(user, "avatar_thumb", None)
        if avatar and getattr(avatar, "url_list", None):
            if avatar.url_list:
                return avatar.url_list[0]
    except Exception:
        pass
    return ""


# =========================================================
# NODE MIDDLEWARE CLIENT (ใช้ JWT Bearer Token)
# =========================================================


class MiddlewareClient:
    def __init__(self, server_url: str, token: str, username: str):
        self.server_url = server_url.rstrip("/")
        self.username = username
        self.token = token

        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })

        retry = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=0.3,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "POST", "DELETE"],
        )

        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=10,
            pool_maxsize=10,
        )

        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        timeout: int = REQUEST_TIMEOUT,
    ) -> bool:
        url = f"{self.server_url}{path}"
        try:
            response = self.session.request(
                method=method,
                url=url,
                json=json_data,
                params=params,
                timeout=timeout,
            )
            response.raise_for_status()
            return True
        except requests.RequestException:
            return False

    def register(self) -> bool:
        return self._request(
            "POST",
            "/register",
            json_data={"username": self.username},
        )

    def push_event(self, event_type: str, data: Dict[str, Any]) -> bool:
        return self._request(
            "POST",
            "/push-event",
            json_data={
                "username": self.username,
                "type": event_type,
                "data": data,
            },
            timeout=2,
        )

    def heartbeat(self) -> bool:
        return self._request(
            "POST",
            "/heartbeat",
            json_data={"username": self.username},
            timeout=2,
        )

    def stop(self) -> bool:
        return self._request(
            "DELETE",
            "/stop",
            params={"username": self.username},
            timeout=3,
        )

    def close(self) -> None:
        self.session.close()


# =========================================================
# HEARTBEAT LOOP
# =========================================================


async def heartbeat_loop(api: MiddlewareClient):
    while not stop_event.is_set():
        api.heartbeat()
        await asyncio.sleep(HEARTBEAT_INTERVAL)


# =========================================================
# MAIN
# =========================================================


async def main(license_key: str):
    global TARGET_USERNAME

    # ── 1. ตรวจสอบ License Key ──────────────────────────────
    emit("status", {
        "connected": False,
        "message": "Validating license key...",
    })

    try:
        activation = activate_license(license_key)
    except Exception as e:
        emit("error", {"message": str(e)})
        sys.exit(1)

    jwt_token = activation["token"]
    tiktok_username = activation.get("tiktokUsername") or activation.get("whitelistedUsername", "")

    if not tiktok_username:
        emit("error", {"message": "No TikTok username found in license"})
        sys.exit(1)

    TARGET_USERNAME = tiktok_username.lstrip("@")

    emit("status", {
        "connected": False,
        "message": f"License OK — connecting to @{TARGET_USERNAME}...",
        "orderId": activation.get("orderId", ""),
        "tiktokUsername": TARGET_USERNAME,
    })

    # ── 2. สร้าง Middleware Client ด้วย JWT ──────────────────
    api = MiddlewareClient(SERVER_URL, jwt_token, TARGET_USERNAME)

    if not api.register():
        emit("error", {"message": "Failed to register with middleware"})
        sys.exit(1)

    # ── 3. TikTok Live Client ─────────────────────────────────
    client = TikTokLiveClient(unique_id=TARGET_USERNAME)

    # =====================================================
    # EVENT HANDLERS
    # =====================================================

    @client.on(ConnectEvent)
    async def on_connect(_: ConnectEvent):
        payload = {
            "connected": True,
            "message": f"Connected: @{TARGET_USERNAME}",
        }
        emit("status", payload)
        api.push_event("status", payload)

    @client.on(DisconnectEvent)
    async def on_disconnect(_: DisconnectEvent):
        emit("status", {"connected": False, "message": "Disconnected"})

    @client.on(LiveEndEvent)
    async def on_live_end(_: LiveEndEvent):
        emit("status", {"connected": False, "message": "Live ended"})
        stop_event.set()

    @client.on(CommentEvent)
    async def on_comment(event: CommentEvent):
        user = extract_user_info(event)
        data = {
            "username": user["username"],
            "nickname": user["nickname"],
            "comment": getattr(event, "comment", ""),
            "profilePictureUrl": user["profilePictureUrl"],
        }
        api.push_event("chat", data)

    @client.on(LikeEvent)
    async def on_like(event: LikeEvent):
        user = extract_user_info(event)
        data = {
            "username": user["username"],
            "nickname": user["nickname"],
            "likeCount": getattr(event, "count", 1),
            "totalLikeCount": getattr(event, "total", 0),
            "profilePictureUrl": user["profilePictureUrl"],
        }
        emit("like", {"user": data["username"], "count": data["likeCount"]})
        api.push_event("like", data)

    @client.on(FollowEvent)
    async def on_follow(event: FollowEvent):
        data = {
            "username": event.user.unique_id,
            "nickname": event.user.nickname,
            "profilePictureUrl": safe_get_avatar_url(event.user),
        }
        api.push_event("follow", data)

    @client.on(GiftEvent)
    async def on_gift(event: GiftEvent):
        if getattr(event.gift, "streaking", False):
            return

        user = extract_user_info(event)
        diamond = (
            getattr(event.gift, "diamond_count", None)
            or getattr(event.gift, "diamondCount", None)
            or 0
        )
        repeat_count = getattr(event, "repeat_count", 1)

        data = {
            "id": str(getattr(event, "id", "0")),
            "giftId": getattr(event.gift, "id", 0),
            "giftName": getattr(event.gift, "name", "Unknown"),
            "username": user["username"],
            "nickname": user["nickname"],
            "diamond": diamond,
            "repeatCount": repeat_count,
            "repeatEnd": True,
            "profilePictureUrl": user["profilePictureUrl"],
        }

        emit("gift", {
            "user": data["username"],
            "gift_name": data["giftName"],
            "count": repeat_count,
            "diamond": diamond,
        })
        api.push_event("gift", data)

    # ── 4. Start Tasks ────────────────────────────────────────
    heartbeat_task = asyncio.create_task(heartbeat_loop(api))
    client_task = asyncio.create_task(client.start())

    try:
        await stop_event.wait()
    finally:
        heartbeat_task.cancel()
        client_task.cancel()

        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass

        try:
            if hasattr(client, "disconnect"):
                await client.disconnect()
            elif hasattr(client, "stop"):
                await client.stop()
        except Exception:
            pass

        api.stop()
        api.close()

        emit("status", {"connected": False, "message": "Stopped"})


# =========================================================
# SIGNAL HANDLER & ENTRY POINT
# =========================================================


def handle_signal(*_):
    stop_event.set()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python main.py <license_key>")
        sys.exit(1)

    license_key_arg = sys.argv[1].strip()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        asyncio.run(main(license_key_arg))
    except KeyboardInterrupt:
        pass