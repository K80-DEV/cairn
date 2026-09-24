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
curl -fLO https://raw.githubusercontent.com/K80-DEV/cairn/main/updates/0.6n/marahome.py
sha256sum marahome.py
# expected: 26774c368a3ef580a46c121191807ac22b9095138fbab5f156f8fb5d758a1d5f (809673 bytes)
```

The manifest itself is Ed25519-signed; the daemon verifies signatures against
a key pinned inside the code, so you never have to trust the transport alone.

## 3. First boot
```bash
python3 marahome.py            # listens on 0.0.0.0:8470
```
Open **http://localhost:8470 on the machine itself** (or behind any TLS you
control — see README about the Secure-cookie rule) and follow the wizard:
you will be asked to read and accept what an AI with local tools can do,
create the owner account, and connect a model.

State lives in `./state/` next to the file. Back it up any time from
Settings → Backup (password-encrypted, everything included).

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
4. **Check** → you should see v0.6n with its notes. **Download** → it stages
   and verifies sha256 + signature. **Install** → the daemon takes a
   pre-update snapshot, gold-boot-smoke-tests the staged file in a scratch
   environment, swaps, and restarts itself.
5. Watch the console/journal: the boot line should read
   `mara-home daemon v0.6n // Basalt // Cinder ... (sha 26774c368a3e)`.
6. The next Check should say you are up to date. From 0.6j1 onward, no URL
   pasting needed — the channel is compiled in.

If anything fails mid-install, the updater keeps the pre-update snapshot in
`backups/pre-update-<stamp>/` and prints how to restore. It refuses to apply
downgrades unless overridden from the CLI (`--update-install --force`).

## 5. Uninstalling
Stop the process. Your entire installation is the `marahome.py` file plus
`state/`. Delete both, or keep `state/` and a future install can import it
(Settings → Backup → Import, or `python3 marahome.py --import-backup <file>`).
