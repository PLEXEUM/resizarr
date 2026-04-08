# Resizarr

**Automatically replace oversized movie files with smaller releases in Radarr — no quality compromises, full automation.**

Resizarr scans your Radarr library for movies that exceed your size rules, finds smaller compatible releases, and lets you (or auto-approves) delete the old file and grab the smaller one — all with one click or fully automated.

---

## ✨ Features

- **Smart size-based scanning** — Finds any smaller release that saves space
- **Universal tracker support** — Works with PixelHD, Beyond-HD, Aither, HDBits, and any tracker using numeric torrent IDs
- **Automatic file deletion** — Old movie file is deleted *before* the new smaller one is grabbed (bypasses Radarr's "existing file meets cutoff" error)
- **Pending Approvals UI** — Review, approve single or batch replacements with full details
- **Manual + Auto mode** — Choose per-scan or set rules for hands-off operation
- **Cancel button + real-time progress** — Stop a running scan anytime
- **Logs viewer** — Auto-refresh, filter, download, and clear logs
- **Clear buttons** — Quickly clear pending list or run history
- **Docker-ready** — One-command deploy with persistent SQLite database
- **Quality downgrade bypass** — Works even when Radarr says the new release is lower quality

---

## How It Works

1. You run a scan (manual or scheduled)
2. Resizarr checks every movie against your size rules
3. It asks Radarr for smaller releases
4. Matching candidates are saved to the **Pending Approvals** table
5. You review and click **Approve** (or enable auto-approve)
6. Resizarr:
   - Deletes the old oversized file
   - Pushes the exact smaller release to Radarr using the correct GUID/download URL
7. Radarr downloads the smaller version and your library is updated

**Supported Trackers**: PixelHD, Beyond-HD, Aither, HDBits, and any tracker with a numeric torrent ID in the URL.

---

## Tech Stack

- **Backend**: FastAPI + Python
- **Frontend**: HTMX + Tailwind CSS (no heavy JS framework)
- **Database**: SQLite (single file, zero config)
- **Scheduler**: APScheduler (for future auto-scans)
- **Container**: Docker + Docker Compose
- **Radarr Integration**: Direct API calls with full error handling

---

## Quick Start (Docker)

```bash
# 1. Clone the repo
git clone https://github.com/PLEXEUM/resizarr.git
cd resizarr

# 2. Start the app
docker-compose up -d

# 3. Open the dashboard
# → http://localhost:7227

---

First-time setup:

Go to the Config section in the dashboard
Enter your Radarr URL and API key
Set your size rules (target threshold, operator, excluded extensions, etc.)
All settings are saved automatically in SQLite

The app will be available at http://localhost:7227 (or whatever port you set in docker-compose.yml).

---

Usage

Dashboard — Overview, run history, quick scan button
Pending Approvals — Review and approve replacements
Logs — Real-time logging with auto-refresh
Run a scan — Click the big Scan button (dry-run mode available)

---

Screenshots
(Coming soon)

---

Roadmap / Wishlist
Current status: Fully functional and production-ready
Planned next features:

Auto-approve rules (seeders, release age, % size savings, IMDb score)
Discord / Telegram notifications
Replacement statistics dashboard (GB saved, average reduction, etc.)
Dry-run CSV export + scheduled reports
Advanced scanner filters (exclude tags, minimum savings threshold)
Scheduled scans via APScheduler

---

Contributing
Pull requests are welcome! Feel free to open an issue for feature requests or bugs.

License
MIT License — see LICENSE for details.

---

Made with ❤️ for the Plex/Radarr community.