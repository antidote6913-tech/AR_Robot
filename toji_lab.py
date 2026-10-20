#!/usr/bin/env python3
"""
toji_lab.py - try Toji's chat style in the terminal, without Telegram or the rest of the bot.

It reads the SYSTEM_PROMPT from your chatbot.py, talks to the same OpenAI-compatible API, and lets you
  * chat with Toji by typing (default), or
  * replay what you typed to the reference bots (Nova / Samina) and read what Toji says to the same lines.
Use it to compare models and styles quickly (--model, --url, --effort, --temp).

Examples
  python toji_lab.py                        # chat
  python toji_lab.py --replay nova          # same user lines as the Nova screenshot
  python toji_lab.py --replay samina --girl # Samina lines, flirt mode on (pretend the user is a girl)
  python toji_lab.py --replay my_lines.txt  # your own file, one user message per line
  python toji_lab.py --model openai/gpt-oss-20b --effort low

Key: CHATBOT_API_KEY (or GROQ_API_KEY). URL/model: --url/--model or CHATBOT_API_URL / CHATBOT_MODEL.
Only the standard library is used. It is a style check: the real bot also trims emojis, splits replies,
adds memory notes and a quality-rewrite pass.
"""
import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

REPLAYS = {
    "nova": [
        "Hello", "Kuch na hua", "Hnn oo to hai tu bata ky kar rha hai", "Kuch na chat karna aya huu",
        "Kuch na bss jinda huu Bhagwan bhrosha", "Interesting to kuch na hai 🤔", "Hello baby", "Kuch na",
        "Ok ok", "Nhi", "[sent a sticker: 🥹 face holding back tears]", "Ok ok", "Hnn", "Ok", "Oo 😂",
    ],
    "samina": [
        "Hello", "Kuch nhi apna batoo", "Do to koi kaam karo", "Do 😂", "Kuch nhi", "Abb adat hoo gaya hai",
        "Do ok", "Classroom of the elite", "And u", "Hnn", "Dono", "Hnn", "Mera mnnn",
        "Do 😂 line daa rhi hoo tum", "Hnn", "Dil se",
    ],
}


def load_prompt(path: Path) -> str:
    text = path.read_text(encoding="utf-8")
    m = re.search(r'SYSTEM_PROMPT = """\\\n(.*?)"""', text, re.S)
    if not m:
        sys.exit(f"could not find SYSTEM_PROMPT in {path}")
    return m.group(1)


def build_system(prompt: str, name: str, girl: bool) -> str:
    notes = [
        "This is a PRIVATE chat with one person.",
        f"Reply to {name}'s latest message.",
        f"{name} is NOT your owner.",
        "This looks like casual chat: reply like a real friend texting: one or two short lines (about 8-22 words) "
        "with a tease, an opinion or a hook. If you use two lines, put each on its own line. No essay.",
    ]
    if girl:
        notes.append(f"FLIRT MODE ON: {name} told you she is a girl. Be playfully, lightly flirty in a casual, fun way "
                     "that fits what she just said. Not serious, not clingy, never explicit.")
    else:
        notes.append("FLIRT MODE OFF: friendly banter only, no flirting.")
    return prompt + "\nCURRENT SITUATION\n" + "\n".join("- " + n for n in notes)


def call_api(url: str, key: str, model: str, messages: list, temp: float, effort: str, max_tokens: int) -> str:
    payload = {"model": model, "messages": messages, "temperature": temp, "top_p": 0.9,
               "max_completion_tokens": max_tokens}
    if effort:
        payload["reasoning_effort"] = effort
    for _ in range(3):
        req = urllib.request.Request(
            url, data=json.dumps(payload).encode(), method="POST",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode())
            return str(data["choices"][0]["message"]["content"] or "").strip()
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")[:300]
            if e.code == 400 and "reasoning_effort" in payload:
                payload.pop("reasoning_effort")
                continue
            if e.code == 400 and "max_completion_tokens" in payload:
                payload["max_tokens"] = payload.pop("max_completion_tokens")
                continue
            sys.exit(f"API error {e.code}: {body}")
        except (urllib.error.URLError, KeyError, IndexError, ValueError) as e:
            sys.exit(f"API call failed: {type(e).__name__}: {e}")
    sys.exit("API call failed after retries")


def clean(text: str) -> str:
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.S | re.I)
    text = re.sub(r"(\*\*|__|~~|`)", "", text)
    text = re.sub(r"(?m)^\s*(?:[-*•]|\d+[.)])\s+", "", text)
    text = re.sub(r"^(?:toji|bot|assistant)\s*:\s*", "", text.strip(), flags=re.I)
    return re.sub(r"\n{2,}", "\n", text).strip()


def main() -> None:
    ap = argparse.ArgumentParser(description="Try Toji's chat style in the terminal")
    ap.add_argument("--chat-file", default=str(Path(__file__).with_name("chatbot.py")), help="path to chatbot.py")
    ap.add_argument("--url", default=os.environ.get("CHATBOT_API_URL", "https://api.groq.com/openai/v1/chat/completions"))
    ap.add_argument("--model", default=os.environ.get("CHATBOT_MODEL", "openai/gpt-oss-120b"))
    ap.add_argument("--key", default=os.environ.get("CHATBOT_API_KEY") or os.environ.get("GROQ_API_KEY") or "")
    ap.add_argument("--temp", type=float, default=0.8)
    ap.add_argument("--effort", default=os.environ.get("CHATBOT_REASONING_EFFORT", "medium"), help='"" to disable')
    ap.add_argument("--max-tokens", type=int, default=1200)
    ap.add_argument("--name", default="Rahul", help="the user's name in the chat")
    ap.add_argument("--girl", action="store_true", help="switch flirt mode on (pretend the user is a girl)")
    ap.add_argument("--replay", help="nova | samina | path to a text file with one user message per line")
    args = ap.parse_args()
    if not args.key:
        sys.exit("set CHATBOT_API_KEY or GROQ_API_KEY (or pass --key)")

    system = build_system(load_prompt(Path(args.chat_file)), args.name, args.girl)
    history: list = []

    def turn(user_text: str) -> None:
        history.append({"role": "user", "content": user_text})
        messages = [{"role": "system", "content": system}] + history[-30:]
        reply = clean(call_api(args.url, args.key, args.model, messages, args.temp, args.effort, args.max_tokens))
        history.append({"role": "assistant", "content": reply})
        print(f"  {args.name}: {user_text}")
        for line in (reply.split("\n") if reply else ["(empty reply)"]):
            print(f"  Toji: {line}")
        print()

    if args.replay:
        lines = REPLAYS.get(args.replay.lower())
        if lines is None:
            lines = [ln.strip() for ln in Path(args.replay).read_text(encoding="utf-8").splitlines() if ln.strip()]
        print(f"model={args.model}  effort={args.effort or '-'}  temp={args.temp}  flirt={'on' if args.girl else 'off'}\n")
        for ln in lines:
            turn(ln)
        return
    print("Chat with Toji (Ctrl+C to quit).\n")
    try:
        while True:
            text = input(f"{args.name}: ").strip()
            if text:
                turn(text)
    except (KeyboardInterrupt, EOFError):
        print()


if __name__ == "__main__":
    main()
