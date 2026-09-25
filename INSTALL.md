# Installing CAIRN (pre-release)

> Read the PRE-RELEASE banner in [README.md](README.md) first. LAN-only for now.

## 1. Requirements
- Linux (x86_64 or Pi/aarch64), Python **3.11+**
- Packages: `sudo apt install python3 python3-cryptography tesseract-ocr ffmpeg`
- Any OpenAI-compatible model endpoint + key (BYOK) — or Ollama on the same box

## 2. Download and verify
Grab the build you want and check its checksum against the manifest the
release was made with (`updates/manifest.json` in this repo is the *current*
release; older ones are in the file's git history):

```bash
curl -fLO https://raw.githubusercontent.com/K80-DEV/cairn/main/updates/0.6v/marahome.py
sha256sum marahome.py
# expected: c6c124c49fb9ceab28ddff2a9607e2a08d392e665d3098120cab3f4177b42a40 (969152 bytes)
```

The manifest itself is Ed25519-signed; the daemon verifies signatures against
a key pinned inside the code, which catches a tampered manifest. The pin ships
inside the file you just downloaded, so to close the loop fully, confirm the
signing key fingerprint over a channel separate from this download.

## 3. First boot
```bash
python3 marahome.py            # listens on 127.0.0.1:8470 (loopback only)
```
Open **http://localhost:8470 on the machine itself** (or behind any TLS you
control — see README about the Secure-cookie rule) and follow the wizard:
you will be asked to read and accept what an AI with local tools can do,
create the owner account, and connect a model.

State lives under `$MARA_HOME/state/` (default `/var/lib/cairn/state/`); the
account registry is `$MARA_REGISTRY` (default `/var/lib/mara/users.db`). Back it
all up any time from Settings → Backup (password-encrypted, everything included).
Current builds export authenticated (v2) backup containers. Older v1 files
carry no integrity signature: current builds verify their structure but
refuse to restore them, because their provenance cannot be trusted. If you
hold a v1 backup, restore it on the version that created it and re-export a
v2 immediately; `--allow-legacy-v1` beside `--import-backup` is the deliberate
one-off escape hatch for emergencies that accept that risk

## 4. Testing the updater (if you're here to do that)
The updater is **off by default** and **pull-only**.
1. Install the previous build (e.g. `updates/0.6j/marahome.py`,
   sha256 `86a26164656c55aa29cadcee1f00d071eb95a0626a20f4bc093ec3b6a60a7040`),
   finish the wizard, sign in as the owner.
2. Settings → System card → enable updates.
3. Set the manifest URL to
   `https://raw.githubusercontent.com/K80-DEV/cairn/main/updates/manifest.json`
   (builds from 0.6j1 on know this URL already; only older builds need it
   pasted once).
4. **Check** → the channel's current release should appear, with its notes. **Download** → it stages
   and verifies sha256 + signature. **Install** → the daemon takes a
   pre-update snapshot, gold-boot-smoke-tests the staged file in a scratch
   environment, swaps, and restarts itself.
5. Watch the console/journal: the boot line should read
   `mara-home daemon v<new-version> // Basalt // Ghostlight (sha <new-sha>)`.
   "Ghostlight" is the held build name for the whole Basalt (0.X) series through 1.0;
   each build differs only by its version letter and sha.
6. The next Check should say you are up to date. From 0.6j1 onward, no URL
   pasting needed — the channel is compiled in.

If anything fails mid-install, the updater keeps the pre-update snapshot in
`backups/pre-update-<stamp>/` and prints how to restore. It refuses to apply
downgrades unless overridden from the CLI (`--update-install --force`).

## 5. Uninstalling
Stop the process. Your entire installation is the `marahome.py` file plus
`state/`. Delete both, or keep `state/` and a future install can import it
(Settings → Backup → Import, or `python3 marahome.py --import-backup <file>`).
