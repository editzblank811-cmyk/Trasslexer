# Hindi Translate Bot (Telegram)

Send a **.epub** or **.txt** file (1 to 15 MB) to the bot. It translates **Chinese or English into Hindi** and sends the file back.

- Live progress bar, **time used** and **time left**
- Many free translate engines work at the same time: Google (3 ways), Bing, Yandex, Lingva mirrors, MyMemory, LibreTranslate mirrors. If one is slow, blocked or wrong, the others take over.
- Every line is checked. A line that still looks untranslated is retried on other engines, then cut into small pieces and retried, and at the end the finished file is scanned once more. The result message says how many lines are fully translated, and a list is attached if any look untranslated.
- EPUB layout, images, fonts and styles are kept. TXT keeps its blank lines, indents and line endings (GBK, Big5, UTF-8 and UTF-16 files are detected).
- No packages to install. Pure Python standard library.

## Setup (2 minutes)

1. In Telegram open **@BotFather**, send `/newbot`, copy the token it gives you.
2. Put the token in one setting called `BOT_TOKEN`. That is the only setting needed.
3. Run `python bot.py`.

### On an Android phone (Termux, free)

```
pkg update && pkg install -y python git
git clone https://github.com/YOUR-NAME/YOUR-REPO
cd YOUR-REPO
export BOT_TOKEN="paste-your-token-here"
termux-wake-lock
python bot.py
```

### With Docker or any server

```
docker build -t hindi-bot .
docker run -d --restart always -e BOT_TOKEN="paste-your-token-here" hindi-bot
```

### Free web hosts (Render, Koyeb, Railway, Fly)

Connect this GitHub repo, set start command `python bot.py`, add `BOT_TOKEN`. If the host gives a `PORT`, the bot opens a small health page on it automatically.

> GitHub only stores the code. Do not run the bot on GitHub Actions runners; GitHub's terms do not allow using them as a bot server. The included `test.yml` workflow only runs the tests (put it at `.github/workflows/test.yml`).

## Optional settings

| Setting | Meaning |
|---|---|
| `MAX_JOBS` | Files translated at the same time (default 2) |
| `DISABLE_ENGINES` | Comma list to switch off, e.g. `yandex,mymemory` |
| `LINGVA_URLS` | Comma list of Lingva mirror URLs |
| `LIBRETRANSLATE_URLS` / `LIBRETRANSLATE_API_KEY` | Your own LibreTranslate server |
| `MYMEMORY_EMAIL` | Raises the free MyMemory daily limit |
| `STALL_SECONDS` | Give up if nothing translates for this long (default 240) |

## Limits

- Max file size 15 MB (Telegram lets bots download 20 MB). DRM-protected EPUBs cannot be translated.
- Speed depends on the free engines and your server's internet. Free engines can rate-limit; the bot spreads load and falls back automatically.
- Names and short words may stay as they are; they are counted separately in the check.

## Tests

`python -m unittest -v` runs 37 tests offline against fake servers.
