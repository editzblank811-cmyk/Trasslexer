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

### Run it on GitHub itself (Actions)

1. Make the repo **Public** (private repos only get 2000 free minutes a month, a bot needs far more).
2. Repo **Settings → Secrets and variables → Actions → New repository secret**. Name: `BOT_TOKEN`, value: your token from BotFather.
3. Put `run-bot.yml` at `.github/workflows/run-bot.yml` (Add file → Create new file, type that path).
4. Open the **Actions** tab → **run-bot** → **Run workflow**. Done. The bot is online.

How it keeps running: one run lasts about 5.5 hours. Then the bot stops taking new files, finishes the ones it is translating (up to 12 minutes), exits, and starts the next run by itself. An hourly schedule is a backup. Only one run is active at a time. Cancelled or waiting runs in the list are normal.

Things to know:
- GitHub's terms say Actions are meant for building and testing software, not for hosting a server. A bot like this can get a warning or be stopped by GitHub. If that happens, run it on your phone (Termux) or a free host instead; the same `bot.py` works everywhere.
- Between two runs there can be a gap of a minute or two. Files sent in that gap wait in Telegram and are picked up when the next run starts.
- If the bot ever stops (GitHub switches off schedules after 60 days without repo activity), just press **Run workflow** again.
- If the token is wrong the run fails and does not restart in a loop. Fix the secret and press **Run workflow**.

## Optional settings

| Setting | Meaning |
|---|---|
| `MAX_JOBS` | Files translated at the same time (default 2) |
| `DISABLE_ENGINES` | Comma list to switch off, e.g. `yandex,mymemory` |
| `LINGVA_URLS` | Comma list of Lingva mirror URLs |
| `LIBRETRANSLATE_URLS` / `LIBRETRANSLATE_API_KEY` | Your own LibreTranslate server |
| `MYMEMORY_EMAIL` | Raises the free MyMemory daily limit |
| `STALL_SECONDS` | Give up if nothing translates for this long (default 240) |
| `MAX_RUNTIME_MIN` / `DRAIN_MIN` | Stop after this many minutes and finish running files first (used by the GitHub workflow) |

## Limits

- Max file size 15 MB (Telegram lets bots download 20 MB). DRM-protected EPUBs cannot be translated.
- Speed depends on the free engines and your server's internet. Free engines can rate-limit; the bot spreads load and falls back automatically.
- Names and short words may stay as they are; they are counted separately in the check.

## Tests

`python -m unittest -v` runs 38 tests offline against fake servers.
