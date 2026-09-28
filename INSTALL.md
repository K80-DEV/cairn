# Installing CAIRN (pre-release)

> Read the PRE-RELEASE banner in [README.md](README.md) first. LAN-only for now.

## 1. Requirements
- Linux (x86_64 or Pi/aarch64), Python **3.11+**
- Packages: `sudo apt install python3 python3-cryptography tesseract-ocr ffmpeg`
- Any OpenAI-compatible model endpoint + key (BYOK) — or Ollama on the same box

## 2. Download and verify
Grab a release from its **immutable tag** (recommended over the mutable
`main` branch: a tag never changes after it is pushed). Each release's file
lives at `updates/<version>/marahome.py` inside its tag, and its bytes are
attested by the signed manifest that shipped with it
(`updates/manifest.json` at that tag):

```bash
curl -fLO https://raw.githubusercontent.com/K80-DEV/cairn/refs/tags/v0.6y/updates/0.6y/marahome.py
sha256sum marahome.py
# expected: 4eaa0f9f4da38039c3ce32092ba84efed77c9eed0bc33d7e9085cfac0a7d26ab (1180897 bytes)
```

> **ALPHA build:** automatic update from older builds is not supported. If you run
> an earlier build, please update manually (download and swap) after taking a
> backup.

The manifest itself is Ed25519-signed; the daemon verifies signatures against
a key pinned inside the code, which catches a tampered manifest. The pin ships
inside the file you just downloaded, so to close the loop fully, confirm the
signing key fingerprint over a channel separate from this download.

The master signing key fingerprint is the lowercase hex sha256 of the raw
32-byte Ed25519 public key - the same formula the daemon itself computes:

`a8d666d48cf3c5915ee715d23c573b790197053209b20ca08a6b91e5b53497ca`

Once installed, your own daemon republishes the fingerprint of the key it
actually uses at `/api/update/status` (`key_fp`, owner-only) - a second look
at the same fact from inside the box, independent of this repo.

## 3. First boot
```bash
mkdir -p ~/cairn
export MARA_HOME="$HOME/cairn/mh"        # state tree (defaults below need root)
export MARA_REGISTRY="$HOME/cairn/reg.db" # account registry
python3 marahome.py            # listens on 127.0.0.1:8470 (loopback only)
```
> The default locations (`/var/lib/cairn`, `/var/lib/mara/users.db`) assume a
> system-service install. Running as a regular user without those two exports
> fails on first boot while creating them — point both at writable paths, or
> run as root.
Open **http://localhost:8470 on the machine itself** (or behind any TLS you
control — see README about the Secure-cookie rule) and follow the wizard:
you will be asked to read and accept what an AI with local tools can do,
create the owner account, and connect a model.
> **Setup token:** on a loopback bind (the default above) the wizard needs no
> token. If you bind the daemon to a non-loopback address *before* an owner
> account exists, the daemon prints a one-use setup token at first boot
> (`SETUP TOKEN (first boot, one use)` in the console/journal) and the wizard
> demands `/setup?t=<token>` — so whoever is on your LAN cannot claim the box
> before you do.

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
   `mara-home daemon v<new-version> // BASALT // GHOSTLIGHT (sha <new-sha>)`.
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
