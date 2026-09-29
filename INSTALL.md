# Installing CAIRN (pre-release)

> Read the PRE-RELEASE banner in [README.md](README.md) first. LAN-only for now.

## 1. Requirements
- Linux (x86_64 or Pi/aarch64), Python **3.11+**
- Packages: `sudo apt install python3 python3-cryptography tesseract-ocr ffmpeg`
- Any OpenAI-compatible model endpoint + key (BYOK) — or Ollama on the same box

## 2. Download and verify
Grab a release from its **immutable tag** (recommended over the mutable
`main` branch: a tag never changes after it is pushed). Each release's file
lives at `updates/<version>/cairn.py` inside its tag, and its bytes are
attested by the signed manifest that shipped with it
(`updates/manifest.json` at that tag):
```bash
curl -fLO https://raw.githubusercontent.com/K80-DEV/cairn/refs/tags/v0.7b/updates/0.7b/cairn.py
sha256sum cairn.py
# expected: d8e6e0aefa445d6f2ea4093b2f9fc3d3aeceaff308acbae3760056d2a97fec24  cairn.py (1234160 bytes)
```
> 0.6x-era tags still ship `marahome.py` — that is the era spelling, not an
> error. Coming from one of those? You arrive through the ferry in §8.

> **Beta builds:** automatic updates do **not** cross the 0.6x → 0.7a rename
> by themselves — that crossing is one command; see §8. Between 0.7-series
> builds the updater works as usual.

The manifest itself is Ed25519-signed; the daemon verifies signatures against
a key pinned inside the code, which catches a tampered manifest. The pin ships
inside the file you just downloaded, so to close the loop fully, confirm the
signing key fingerprint over a channel separate from this download.
The master signing key fingerprint is the lowercase hex sha256 of the raw
32-byte Ed25519 public key - the same formula the daemon itself computes:
`d7ea6e53800fe2475edee2e7604689b116a3f0908bbca9ddf80d76069ef91d8a` (the release key minted at the 0.7a key ceremony; builds v0.6y
and older verified the dev-era key `a8d666d4…` — if a 0.7a-or-newer build
quotes you that older fingerprint, the bytes are stale).

Once installed, your own daemon republishes the fingerprint of the key it
actually uses at `/api/update/status` (`key_fp`, owner-only) - a second look
at the same fact from inside the box, independent of this repo.

## 3. First boot
```bash
mkdir -p ~/cairn
export CAIRN_HOME="$HOME/cairn/mh"          # state tree (defaults below need root)
export CAIRN_REGISTRY="$HOME/cairn/reg.db"  # account registry
python3 cairn.py             # listens on 127.0.0.1:8470 (loopback only)
```
> The default locations (everything under `/var/lib/cairn`) assume a
> system-service install. Running as a regular user without those two exports
> fails on first boot while creating them — point both at writable paths, or
> run as root (or use the systemd unit in §5, which sets them).

> **Legacy spelling:** `MARA_HOME` / `MARA_REGISTRY` are still read for
> exactly one release (the `CAIRN_*` name wins when both are set). They go
> away in 0.7b — set the new spellings now.

Prove it is alive before touching a browser:
```bash
curl -sf http://127.0.0.1:8470/health
```

Open **http://localhost:8470 on the machine itself** (or behind any TLS you
control — see README about the Secure-cookie rule) and follow the wizard:
you will be asked to read and accept what an AI with local tools can do,
create the owner account, connect a model, and decide where your vault key
lives (§4 — you can defer, but read it first).

> **Setup token (one time, one use):** on a loopback bind (the default above)
> the wizard needs no token. If you bind the daemon to a non-loopback address
> *before* an owner account exists, the daemon prints a one-use setup token
> at first boot (`SETUP TOKEN (first boot, one use)` in the console/journal —
> printed once, never repeated) and the wizard demands `/setup?t=<token>` —
> so whoever is on your LAN cannot claim the box before you do.

State lives under `$CAIRN_HOME/state/` (default `/var/lib/cairn/state/`); the
account registry is `$CAIRN_REGISTRY` (default `/var/lib/cairn/users.db`). Back it
all up any time from Settings → Backup (password-encrypted, everything included).
Current builds export authenticated (v2) backup containers. Older v1 files
carry no integrity signature: current builds verify their structure but
refuse to restore them, because their provenance cannot be trusted. If you
hold a v1 backup, restore it on the version that created it and re-export a
v2 immediately; `--allow-legacy-v1` beside `--import-backup` is the deliberate
one-off escape hatch for emergencies that accept that risk.

## 4. The lockbox: where the vault key lives
Every secret the agent stores is sealed with one 4096-byte master key. The
**lockbox** is the owner-only page (`vault-key`) where you choose where that
key lives — offered as a wizard step on first boot, and reachable **forever**
afterwards. Skipping is legal: everything else keeps working, and only
vault-backed secrets return 503 with a pointer to that page.

The daemon walks this ladder on every use, first readable file wins, and
re-reads the key file **every time** (no cached copy — that is what makes a
pulled drive fail closed):

| Order | Location | What it means |
|---|---|---|
| 1 | `VAULT_KEY_PATH` env | When set, it IS the ladder: explicit means exact. No fallback — if that file is missing, the vault stays sealed rather than silently using another key. |
| 2 | Pointer file `vault-key-path.conf` beside your data | One line, one path, written when the lockbox page adopts a key. A path is not a secret; key bytes never live here and are never logged. |
| 3 | `/run/cairn-vault.key` | tmpfs: the key dies at reboot. The vault returns **sealed until restaged** — fine on a box you sit in front of, annoying on a headless one. |
| 4 | `vault.key` beside the data (generated on-box) | Stops an escaped backup or a stolen database file; does NOT stop anyone who owns the box. A lock on the drawer, not a safe in the bank. |

**USB stick tier (wizard offers it):** a key on a stick is a house key left
in the door while the box runs. **Lose the stick, lose the secrets** — there
is no recovery path, by design. The wizard forces a **second-stick copy**
before it will finish; store the clone somewhere the death of the machine
cannot reach. Unplug mid-use and vault features **fail closed** until it
returns; reboot seals the same way until the stick (or pointer) is restaged.

**Bring your own key** (any path the daemon can read):
```bash
head -c 4096 /dev/urandom > /run/cairn-vault.key
chmod 600 /run/cairn-vault.key
```
Open the lockbox page and check that path — green means 4096 readable bytes.
The page records the path, never the bytes. **Key bytes are never logged,
returned, or echoed anywhere**, on any of these tiers; a generation step that
finds a live key refuses (409) rather than overwrite — rotation is a manual,
documented act, never an accident.

The built-in Help Center mirrors all of this under the topic **The Lockbox**.

## 5. Running as a service (systemd)
Proven shape (Debian 13; runs as an unprivileged user, loopback bind):
```ini
[Unit]
Description=CAIRN daemon
After=network.target

[Service]
User=cairn
WorkingDirectory=/var/lib/cairn
Environment=CAIRN_HOME=/var/lib/cairn
Environment=CAIRN_REGISTRY=/var/lib/cairn/users.db
ExecStart=/usr/bin/python3 /opt/cairn/cairn.py
Restart=on-failure

[Install]
WantedBy=multi-user.target
```
Adjust `User=`, paths, and the `ExecStart` filename to your box. If the vault
key lives at `/run/cairn-vault.key`, either stage it where this user can read
it or add a `RuntimeDirectory`/`ExecStartPre` of your own — the daemon never
creates key material for you.

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now cairn.service
curl -sf http://127.0.0.1:8470/health
journalctl -u cairn -e      # boot line prints version + build sha
```

## 6. Putting it behind a door you control (Caddy)
The daemon binds loopback only and stays there. Everything public goes through
a reverse proxy that terminates TLS. Two kinds of routes:

- **The front desk** — unauthenticated pages (`/login`, the first-boot wizard,
  PWA install plumbing). The front desk must be self-sufficient at the site
  root: `/` redirects to `/login`, and the manifest/service-worker/icon/setup
  paths are proxied to the daemon directly.
- **Agent doors** — `/<slug>/` reaches one agent. A door should only be
  opened for a slug that is an actual agent account: the daemon verifies the
  slug header against the registry and 404s ghosts, but do not write handles
  for residents that do not exist.

```caddyfile
cairn.example.org {
    # front desk (apex)
    redir / /login 301
    handle /login*      { reverse_proxy 127.0.0.1:8470 }
    handle /signup*     { reverse_proxy 127.0.0.1:8470 }
    handle /api/login*  { reverse_proxy 127.0.0.1:8470 }
    handle /api/signup* { reverse_proxy 127.0.0.1:8470 }
    handle /manifest.webmanifest { reverse_proxy 127.0.0.1:8470 }
    handle /sw.js       { reverse_proxy 127.0.0.1:8470 }
    handle /static/color.png    { reverse_proxy 127.0.0.1:8470 }
    handle /setup       { reverse_proxy 127.0.0.1:8470 }
    handle /api/setup/* { reverse_proxy 127.0.0.1:8470 }

    # one door, one real agent (repeat per agent)
    handle /testbed/* {
        uri strip_prefix /testbed
        reverse_proxy 127.0.0.1:8470 {
            header_up X-Cairn-Slug testbed
        }
    }
}
```
This shape is proven on Caddy 2.11.4 (upstream package) **and** on the ancient
2.6.2 that ships in the Debian apt repo — both work.

> **Debian apt trap:** `apt install caddy` immediately starts a default-site
> Caddy on :80 (admin API on). Editing `/etc/caddy/Caddyfile` and running
> `systemctl enable --now caddy` does **not** pick your config up — it silently
> keeps serving the old one. The edit cycle is:
> `caddy validate --config /etc/caddy/Caddyfile` → `sudo systemctl restart caddy`
> (restart, not reload) → probe your front desk.

## 7. The updater (if you're here to do that)
The updater is **off by default** and **pull-only**.
1. Finish the wizard, sign in as the owner.
2. Settings → System card → enable updates.
3. Set the manifest URL to
   `https://raw.githubusercontent.com/K80-DEV/cairn/main/updates/manifest.json`
   (recent builds know this URL already; only older builds need it pasted once).
4. **Check** → the channel's current release should appear, with its notes.
   **Download** → it stages and verifies sha256 + signature. **Install** → the
   daemon takes a pre-update snapshot, gold-boot-smoke-tests the staged file
   in a scratch environment, swaps, and restarts itself.
5. Watch the console/journal: the boot line should read
   `cairn daemon v<new-version> // BASALT // GHOSTLIGHT (sha <new-sha>)`.
   "Ghostlight" is the held build name for the whole Basalt (0.X) series through 1.0;
   each build differs only by its version letter and sha.
6. The next Check should say you are up to date.
If anything fails mid-install, the updater keeps the pre-update snapshot in
`backups/pre-update-<stamp>/` and prints how to restore. It refuses to apply
downgrades unless overridden from the CLI (`--update-install --force`).

> The updater never renames your running file. Updating *across* the 0.6x →
> 0.7a rename leaves you with new bytes under the old name until you run the
> ferry below — that is by design; the daemon does not rename the floor it is
> standing on.

## 8. Crossing the river (0.6x → 0.7a)
The renamed era moved: the file (`marahome.py` → `cairn.py`), the environment
spellings (`MARA_*` → `CAIRN_*`), the state roots (`/var/lib/mara` →
`/var/lib/cairn`, `/etc/mara` → `/etc/cairn`), and the door header
(`X-Mara-Slug` → `X-Cairn-Slug`). Old spellings are accepted for exactly one
release; **0.7b drops them.**

`cairn-migrate.py` ships with the release (download from the release channel or use the copy in `updates/0.7a/`). Run it once, as root:
```bash
curl -fLO https://raw.githubusercontent.com/K80-DEV/cairn/refs/tags/v0.7a/updates/0.7a/cairn-migrate.py
sudo python3 cairn-migrate.py --dry-run   # reads reality, writes nothing
sudo python3 cairn-migrate.py             # the real crossing
```
What the real run does, in order: takes its **own fresh backup first**
(`/root/cairn-migrate-backup-<ts>/`, mode 600, with SHA256SUMS) → stops the
service → moves the data directories whole (only if the targets are absent) →
installs `cairn.py` beside the old file (kept as `marahome.py.bak-old-name`) →
rewrites the systemd unit with the new spellings (**your `User=` line is never
touched**) → rewrites Caddy doors, but only lines matching the exact known
`X-Mara-Slug` shape, validated *before* the swap (a mismatch is left alone
with a loud manual TODO) → starts the service and proves boot line + `/health`.

- **Second run refuses** via the marker `/etc/cairn/migrate.done` — this is a
  one-time ferry, not a state.
- Registry rows, credentials, agent names and slugs, and vault keys are never
  modified: rehearsed crossings logged in with pre-migration credentials and
  decrypted a pre-migration secret on the far bank, byte-identical. A sealed
  key crosses sealed.
- Accepted one-time scars of the crossing: the theme choice resets once
  (`cairn-theme`), and everyone signs in one more time (session cookie
  renamed).
- Rollback: stop the service, restore the script's own backup set, restart.

## 9. Uninstalling
Stop the process or disable the unit. Your entire installation is the
`cairn.py` file plus the state directory (`$CAIRN_HOME`). Delete both, or
keep the state directory and a future install can import it (Settings →
Backup → Import, or `python3 cairn.py --import-backup <file>`).