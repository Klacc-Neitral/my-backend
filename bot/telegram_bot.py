import json
import time
import urllib.request


# 1) Вставьте сюда токен от BotFather, например: "123456:ABC-DEF..."
BOT_TOKEN = "8333931915:AAFYWlMTinj6-KyUsF7SkTcvHuIKDUJSJYE"

# 2) Вставьте сюда HTTPS-ссылку на ваше мини-приложение (Web App)
# Например: "https://example.com/index.html"
MINI_APP_URL = ""


API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"


def tg(method: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload or {}).encode("utf-8")
    req = urllib.request.Request(
        f"{API_BASE}/{method}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        body = resp.read().decode("utf-8")
    result = json.loads(body)
    if not result.get("ok"):
        raise RuntimeError(result.get("description", "Telegram API error"))
    return result["result"]


def send_mini_app_button(chat_id: int) -> None:
    tg(
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": "Нажмите кнопку, чтобы открыть мини‑приложение:",
            "reply_markup": {
                "inline_keyboard": [
                    [
                        {
                            "text": "Открыть мини‑приложение",
                            "web_app": {"url": MINI_APP_URL},
                        }
                    ]
                ]
            },
        },
    )


def main() -> None:
    if not BOT_TOKEN or not MINI_APP_URL:
        print("Ошибка: заполните BOT_TOKEN и MINI_APP_URL в bot/telegram_bot.py")
        return

    tg("getMe")

    offset = 0
    while True:
        try:
            updates = tg(
                "getUpdates",
                {"offset": offset, "timeout": 30, "allowed_updates": ["message"]},
            )
            for upd in updates:
                offset = upd["update_id"] + 1
                msg = upd.get("message") or {}
                text = msg.get("text") or ""
                chat = msg.get("chat") or {}
                chat_id = chat.get("id")

                if isinstance(text, str) and (text.startswith("/start") or text.startswith("/app")):
                    if isinstance(chat_id, int):
                        send_mini_app_button(chat_id)
        except Exception:
            time.sleep(1)


if __name__ == "__main__":
    main()

