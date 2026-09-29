#!/usr/bin/env python3
"""
cairn-migrate.py - the filename-river ferry (R4, v07a campaign).

Moves a 0.6x-era marahome install into the cairn era, ONE shot:

  marahome.py    -> cairn.py            (old file kept as marahome.py.bak-old-name)
  /var/lib/mara  -> /var/lib/cairn      (only if the target does not exist)
  /etc/mara      -> /etc/cairn          (only if the target does not exist)
  unit file      -> MARA_* env becomes CAIRN_*, ExecStart points at cairn.py.
                    User= is NEVER touched (OS user follows the resident, not the brand).
  Caddyfile      -> 'header_up X-Mara-Slug <slug>' rewritten to X-Cairn-Slug
                    ONLY on exact shape match; backup + caddy validate before
                    reload; on any doubt: loud MANUAL TODO and continue-safe.

NEVER touched: registry rows, vault key contents, /run/*, the User= line.
Data directories MOVE WHOLE - a vault.key inside travels byte-identical.
Refuses to run twice (marker file). Takes its OWN fresh backup before its
first write. Dry-run: --dry-run. Unit override: --unit NAME.service.

Stdlib only. Run as root. Exit codes: 0 done/nothing-to-do, 1 refused/aborted.
"""
import glob
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import time
import urllib.request

TS = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
DRY = "--dry-run" in sys.argv
UNIT = "marahome.service"
_args = sys.argv[1:]
for i, a in enumerate(_args):
    if a == "--unit" and i + 1 < len(_args):
        UNIT = _args[i + 1]
# U14 (2026-09-29, R5 eve - Juniper multi-agent incident):
#   --install-file PATH   the cairn.py that lands is PATH's bytes (the new era
#                         daemon), not a copy of the old file. Refuses if PATH
#                         lacks X-Cairn-Slug dual-accept (a v0.6-era file under
#                         CAIRN_* unit env would boot blind and crash).
#   --caddy-header        OPT-IN rewrite of header_up X-Mara-Slug -> X-Cairn-Slug.
#                         DEFAULT: Caddyfile untouched. 0.6-era agent COPIES only
#                         accept X-Mara-Slug; the 0.7a daemon dual-accepts, so
#                         keeping the old tag is the safe crossing (plumbing keeps
#                         the family name - K80 ruling 2026-09-28).
# Multi-agent: if marahome@.service exists, its EnvironmentFile path follows the
# /etc/mara move, agent env files gain MARA_REGISTRY=<moved registry>, the shared
# registry is made sweep-immune (root:<gid> 0660) AND its parent dir group-writable (U15, sqlite journals), and active agents are stopped
# before the dirs move and restarted + proven after the main daemon is healthy.
CADDY_HEADER = "--caddy-header" in _args
INSTALL_SRC = None
for i, a in enumerate(_args):
    if a == "--install-file" and i + 1 < len(_args):
        INSTALL_SRC = _args[i + 1]
if INSTALL_SRC:
    if not os.path.isfile(INSTALL_SRC):
        print("ABORT        --install-file %s is not a readable file" % INSTALL_SRC)
        sys.exit(1)
    _b = open(INSTALL_SRC, "rb").read()
    if b"X-Cairn-Slug" not in _b:
        print("ABORT        --install-file %s has no X-Cairn-Slug dual-accept - that is an old-era" % INSTALL_SRC)
        print("             file; with the unit rewritten to CAIRN_* env it would crash at boot.")
        print("             Point --install-file at the 0.7a daemon.")
        sys.exit(1)
BACKUP_DIR = "/root/cairn-migrate-backup-" + TS
CADDY = "/etc/caddy/Caddyfile"
MARKERS = ["/etc/cairn/migrate.done", "/var/lib/cairn/.r4-migrated"]
ENV_NAMES = re.compile(r"\bMARA_(HOME|REGISTRY|OWNER|PORT|HOST|TIER)\b")
CADDY_LINE = re.compile(r"(?m)^(\s*)header(_| )up X-Mara-Slug (\S+)\s*$")
backup_files = []


def say(tag, msg):
    print("%-14s %s" % (tag, msg))


def die(msg):
    say("ABORT", msg)
    sys.exit(1)


def sh(cmd, check=False):
    say("$", " ".join(cmd))
    if DRY:
        class R:
            returncode, stdout, stderr = 0, "", ""
        return R()
    r = subprocess.run(cmd, capture_output=True, text=True)
    if check and r.returncode != 0:
        die("command failed: %s\n%s" % (" ".join(cmd), (r.stderr or r.stdout).strip()))
    return r


def ro(cmd):
    # read-only probe: runs even in dry-run, so the dry-run previews REALITY
    return subprocess.run(cmd, capture_output=True, text=True)


def sha16(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def atomic_write(path, text, mode=None):
    tmp = path + ".new"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    if mode is not None:
        os.chmod(tmp, mode)
    os.replace(tmp, path)


# ---------------- guard: one shot ----------------
for m in MARKERS:
    if os.path.exists(m):
        say("REFUSED", "migration marker present (%s). This ferry crosses the river once." % m)
        sys.exit(1)
if not DRY and os.geteuid() != 0:
    die("must run as root (systemctl + /var/lib + /etc)")

# ---------------- plan: unit ----------------
r = ro(["systemctl", "show", "-p", "FragmentPath", "--value", UNIT])
frag = r.stdout.strip()
if r.returncode != 0 or not frag or not os.path.exists(frag):
    die("unit %s not found (FragmentPath empty)" % UNIT)
unit_text = open(frag, encoding="utf-8").read()
em = re.search(r"(?m)^ExecStart=(.*)$", unit_text)
if not em:
    die("no ExecStart= in %s; refusing to guess" % frag)
argv = em.group(1).strip().split()
if argv and argv[0] in ("-", "@", "+"):  # systemd exec modifiers
    argv[0] = argv[0].lstrip("-@+")
py_candidates = [a for a in argv if a.endswith(".py")]
if len(py_candidates) != 1:
    die("ExecStart %r has %d .py token(s); expected exactly one script - refusing to guess"
        % (em.group(1).strip(), len(py_candidates)))
exec_path = py_candidates[0]
if not os.path.exists(exec_path):
    die("ExecStart file %s does not exist" % exec_path)

bn = os.path.basename(exec_path)
cairn_py = os.path.join(os.path.dirname(exec_path), "cairn.py")
if bn not in ("marahome.py", "cairn.py"):
    die("ExecStart basename is %r, neither marahome.py nor cairn.py - nothing here is safe to assume" % bn)
if bn == "cairn.py":
    say("skip", "ExecStart already points at cairn.py")
    rename_code = False
else:
    if os.path.exists(cairn_py):
        die("%s already exists next to %s - refusing to overwrite anything" % (cairn_py, exec_path))
    rename_code = True

data_moved = etc_moved = False
if os.path.isdir("/var/lib/mara"):
    if os.path.exists("/var/lib/cairn"):
        die("/var/lib/mara AND /var/lib/cairn both exist - no blind merge, decide by hand")
    data_moved = True
if os.path.isdir("/etc/mara"):
    if os.path.exists("/etc/cairn"):
        die("/etc/mara AND /etc/cairn both exist - no blind merge, decide by hand")
    etc_moved = True

# ---------------- backup FIRST ----------------
say("backup", BACKUP_DIR)
if not DRY:
    os.makedirs(BACKUP_DIR, mode=0o700)
    for src in (frag, CADDY, exec_path):
        if src and os.path.exists(src):
            dst = os.path.join(BACKUP_DIR, src.replace("/", "_"))
            shutil.copy2(src, dst)
            backup_files.append(dst)
    for d in ("/var/lib/mara", "/etc/mara"):
        if os.path.isdir(d):
            t = os.path.join(BACKUP_DIR, d.replace("/", "_") + ".tar.gz")
            with tarfile.open(t, "w:gz") as tf:
                tf.add(d)
            backup_files.append(t)
    with open(os.path.join(BACKUP_DIR, "SHA256SUMS"), "w") as f:
        for p in backup_files:
            f.write("%s  %s\n" % (hashlib.sha256(open(p, "rb").read()).hexdigest(), p))
    os.chmod(os.path.join(BACKUP_DIR, "SHA256SUMS"), 0o600)
else:
    say("PLAN", "backup unit + Caddyfile + daemon file + tars of /var/lib/mara /etc/mara")

# ---------------- agents (U14): capture BEFORE anything stops ----------------
TPL = "/etc/systemd/system/marahome@.service"
AGENT = "marahome@"
agents = []
_agdir = "/etc/mara/agents"
if os.path.isdir(_agdir):
    for _envf in sorted(glob.glob(os.path.join(_agdir, "*.env"))):
        _slug = os.path.basename(_envf)[:-4]
        _port = ""
        try:
            for _line in open(_envf):
                if _line.strip().startswith("MARA_PORT="):
                    _port = _line.strip().split("=", 1)[1].strip()
        except OSError:
            pass
        agents.append((_slug, _port))
agents_active = [a for a in agents if ro(["systemctl", "is-active", "--quiet", AGENT + a[0]]).returncode == 0]
if agents:
    say("agents", "%d provisioned; active now: %s" % (len(agents), ",".join(s for s, _ in agents_active) or "none"))
# ---------------- stop service ----------------
was_active = subprocess.run(["systemctl", "is-active", "--quiet", UNIT]).returncode == 0 if not DRY else True
sh(["systemctl", "stop", UNIT])
for _slug, _ in agents_active:
    sh(["systemctl", "stop", AGENT + _slug])

# ---------------- move dirs ----------------
def move_dir(old, new):
    sh(["mv", old, new])
    if not DRY:
        if os.path.exists(old):
            die("mv left %s behind; stopping before anything else moves" % old)

if data_moved:
    say("move", "/var/lib/mara -> /var/lib/cairn (whole; vault.key inside is NOT opened)")
    move_dir("/var/lib/mara", "/var/lib/cairn")
else:
    say("skip", "/var/lib/mara not present")
if etc_moved:
    say("move", "/etc/mara -> /etc/cairn")
    move_dir("/etc/mara", "/etc/cairn")
else:
    say("skip", "/etc/mara not present")

# ---------------- install cairn.py ----------------
if rename_code:
    src_py = INSTALL_SRC if INSTALL_SRC else exec_path
    say("install", "%s -> cairn.py (source: %s), old kept as marahome.py.bak-old-name" % (exec_path, src_py))
    if not DRY:
        shutil.copy2(src_py, cairn_py)
        os.chmod(cairn_py, os.stat(exec_path).st_mode)
        os.rename(exec_path, exec_path + ".bak-old-name")
elif INSTALL_SRC:
    say("MANUAL-TODO", "ExecStart already points at cairn.py; --install-file NOT applied over an existing cairn.py (one-shot safety). Swap by hand if that is really what you want.")

# ---------------- rewrite unit ----------------
new_text = unit_text
if rename_code:
    new_text = new_text.replace(exec_path, cairn_py)
new_text = ENV_NAMES.sub(r"CAIRN_\1", new_text)
if data_moved:
    new_text = new_text.replace("/var/lib/mara", "/var/lib/cairn")
if etc_moved:
    new_text = new_text.replace("/etc/mara", "/etc/cairn")
if re.search(r"(?m)^User=", unit_text):
    u_old = re.search(r"(?m)^User=(.*)$", unit_text).group(1)
    u_new = re.search(r"(?m)^User=(.*)$", new_text).group(1)
    if u_old != u_new:
        die("internal error: User= changed during rewrite - report this bug and restore from backup")
if new_text != unit_text:
    if DRY:
        say("PLAN", "rewrite %s: ExecStart->cairn.py, MARA_*->CAIRN_*, moved paths; User= untouched; daemon-reload" % UNIT)
    else:
        bak = frag + ".bak-migrate-" + TS
        shutil.copy2(frag, bak)
        backup_files.append(bak)
        atomic_write(frag, new_text, mode=os.stat(frag).st_mode)
        say("unit", "%s rewritten (User= untouched); old kept at %s" % (UNIT, bak))
        sh(["systemctl", "daemon-reload"], check=True)
else:
    say("unit", "already cairn-era, left byte-identical")
# ---------------- drop-ins (U16: marahome.service.d/pathsfixed.conf scar) ----
# R5 real crossing 2026-09-29: prod health FAILED post-move because a drop-in
# pinned MARA_REGISTRY=/var/lib/mara/users.db. The unit file was clean; the
# pin lived in <unit>.d/. U16 rewrites MOVED PATHS inside drop-ins too.
# Deliberately path-only: env NAMES stay as-is (MARA_* is dual-read by the
# 0.7a daemon; renaming names here would break a rollback to 0.6-era bytes).
_ddir = frag + ".d"
if os.path.isdir(_ddir):
    for _dn in sorted(os.listdir(_ddir)):
        if not _dn.endswith(".conf"):
            continue
        _dp = os.path.join(_ddir, _dn)
        try:
            _dt = open(_dp, encoding="utf-8").read()
        except OSError:
            continue
        _nt = _dt
        if rename_code:
            _nt = _nt.replace(exec_path, cairn_py)
        if data_moved:
            _nt = _nt.replace("/var/lib/mara", "/var/lib/cairn")
        if etc_moved:
            _nt = _nt.replace("/etc/mara", "/etc/cairn")
        if _nt != _dt:
            if DRY:
                say("PLAN", "rewrite drop-in %s: moved paths only, env NAMES untouched (U16)" % _dn)
            else:
                dbak = _dp + ".bak-migrate-" + TS
                shutil.copy2(_dp, dbak)
                backup_files.append(dbak)
                atomic_write(_dp, _nt, mode=os.stat(_dp).st_mode)
                say("unit", "drop-in %s rewritten (paths only, names untouched); backup %s" % (_dn, dbak))
                sh(["systemctl", "daemon-reload"], check=True)
# ---------------- template unit + agent envs + registry bridge (U14) ----------------
agents_report = {"template": "absent", "envs": [], "registry_perm": "n/a", "restarted": []}
if os.path.exists(TPL):
    agents_report["template"] = "present"
    t_text = open(TPL, encoding="utf-8").read()
    new_t = t_text.replace("/etc/mara/", "/etc/cairn/") if etc_moved else t_text
    if new_t != t_text:
        if DRY:
            say("PLAN", "rewrite %s: /etc/mara/ -> /etc/cairn/ ; User=%%i untouched; daemon-reload" % TPL)
        else:
            tbak = TPL + ".bak-migrate-" + TS
            shutil.copy2(TPL, tbak)
            backup_files.append(tbak)
            atomic_write(TPL, new_t, mode=os.stat(TPL).st_mode)
            u_o = re.search(r"(?m)^User=.*$", t_text)
            u_n = re.search(r"(?m)^User=.*$", open(TPL, encoding="utf-8").read())
            if not u_o or not u_n or u_o.group(0) != u_n.group(0):
                die("internal error: User=%%i changed in template - restore %s and report this bug" % tbak)
            sh(["systemctl", "daemon-reload"], check=True)
            say("unit", "%s rewritten (User=%%i untouched); backup %s" % (TPL, tbak))
            agents_report["template"] = "rewritten"
    _adir = "/etc/cairn/agents" if etc_moved else "/etc/mara/agents"
    _reg_target = "/var/lib/cairn/users.db" if data_moved else "/var/lib/mara/users.db"
    if os.path.isdir(_adir):
        for _envf in sorted(glob.glob(os.path.join(_adir, "*.env"))):
            _txt = open(_envf).read()
            if re.search(r"(?m)^(?:MARA|CAIRN)_REGISTRY=", _txt):
                agents_report["envs"].append(os.path.basename(_envf) + ": registry line present")
                continue
            if DRY:
                say("PLAN", "append MARA_REGISTRY=%s to %s" % (_reg_target, _envf))
                continue
            ebak = _envf + ".bak-migrate-" + TS
            shutil.copy2(_envf, ebak)
            backup_files.append(ebak)
            atomic_write(_envf, _txt.rstrip("\n") + "\nMARA_REGISTRY=" + _reg_target + "\n", mode=os.stat(_envf).st_mode)
            say("agents", "%s += MARA_REGISTRY=%s (0.6-era instances read MARA_*)" % (_envf, _reg_target))
            agents_report["envs"].append(os.path.basename(_envf) + ": +MARA_REGISTRY")
    if agents and os.path.exists(_reg_target):
        _st = os.stat(_reg_target)
        _gid = _st.st_gid
        _rdir = os.path.dirname(_reg_target)
        _dst = os.stat(_rdir)
        _need_file = _st.st_uid != 0 or _st.st_mode & 0o077 != 0o060
        _need_dir = (_dst.st_gid != _gid) or not (_dst.st_mode & 0o020)
        if _need_file or _need_dir:
            if DRY:
                say("PLAN", "registry perms -> root:%d 0660; dir %s -> gid %d +g+w (U15)" % (_gid, _rdir, _gid))
            else:
                if _need_file:
                    os.chown(_reg_target, 0, _gid)
                    os.chmod(_reg_target, 0o660)
                if _need_dir:
                    os.chown(_rdir, -1, _gid)          # U15: preserve owner, only re-group
                    os.chmod(_rdir, _dst.st_mode | 0o020)
                say("agents", "registry perms -> root:%d 0660 (sweep-immunity, S3p-v2b); dir %s -> gid %d +g+w (U15: sqlite needs a writable DIR for journals, not just a writable file)" % (_gid, _rdir, _gid))
                agents_report["registry_perm"] = "file root:%d 0660, dir gid %d g+w" % (_gid, _gid)
        else:
            agents_report["registry_perm"] = "already sweep-immune (file+dir)"

# ---------------- Caddy ----------------
caddy_state = "absent"
if os.path.exists(CADDY):
    text = open(CADDY, encoding="utf-8").read()
    hits = CADDY_LINE.findall(text)
    if not hits:
        caddy_state = "no X-Mara-Slug lines; untouched"
        say("caddy", caddy_state)
    elif not CADDY_HEADER:
        caddy_state = "present, NOT rewritten (opt-in --caddy-header)"
        say("caddy", caddy_state + " - X-Mara-Slug is dual-accepted by the 0.7a daemon and REQUIRED by 0.6-era agent copies; leaving it is the safe crossing")
    else:
        new_c = CADDY_LINE.sub(lambda m: "%sheader%sup X-Cairn-Slug %s" % (m.group(1), m.group(2), m.group(3)), text)
        tmp = CADDY + ".new-" + TS
        say("caddy", "%d X-Mara-Slug line(s) matched; validating rewrite first" % len(hits))
        if not DRY:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(new_c)
            os.chmod(tmp, os.stat(CADDY).st_mode)
            v = sh(["caddy", "validate", "--config", tmp])
            if v.returncode != 0:
                os.unlink(tmp)
                caddy_state = "MANUAL TODO (validate failed; file untouched)"
                say("MANUAL-TODO", "caddy validate rejected the rewrite - Caddyfile NOT touched, reload SKIPPED. Fix by hand: header_up X-Mara-Slug -> X-Cairn-Slug, then 'caddy validate' and RESTART (not reload) caddy.")
            else:
                bak = CADDY + ".bak-migrate-" + TS
                shutil.copy2(CADDY, bak)
                backup_files.append(bak)
                os.replace(tmp, CADDY)
                caddy_state = "rewritten + reloaded"
                rc = sh(["systemctl", "restart", "caddy"]).returncode  # restart, NOT reload (Debian trap)
                if rc != 0:
                    caddy_state = "rewritten but RESTART FAILED - fix by hand, backup at " + bak
                    say("MANUAL-TODO", caddy_state)
        else:
            say("PLAN", "caddy: rewrite validated temp copy, backup, swap, systemctl RESTART caddy")
else:
    say("caddy", "no /etc/caddy/Caddyfile; skipping")

# ---------------- start + proof ----------------
pm = re.search(r"(?m)^(?:Environment=)?(?:CAIRN|MARA)_PORT=(\d+)", new_text)
port = int(pm.group(1)) if pm else 8470
if DRY:
    say("PLAN", "start service, poll http://127.0.0.1:%d/health, journal proof, write marker" % port)
sh(["systemctl", "start", UNIT])
health = "not-checked(dry)"
if not DRY:
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            with urllib.request.urlopen("http://127.0.0.1:%d/health" % port, timeout=2) as resp:
                if resp.status == 200:
                    health = "200"
                    break
        except Exception:
            time.sleep(1)
    if health != "200":
        say("MANUAL-TODO", "service did NOT answer /health within 20s. Restore recipe: stop %s, put %s back (see %s), daemon-reload, start." % (UNIT, frag + ".bak-migrate-" + TS, BACKUP_DIR))
        sys.exit(1)
    j = subprocess.run(["journalctl", "-u", UNIT, "-n", "3", "--no-pager", "-o", "cat"],
                       capture_output=True, text=True).stdout.strip()
    say("journal", j.replace("\n", " | "))
# ---------------- agents restart + proof (U14) ----------------
if DRY:
    for _slug, _port in agents_active:
        say("PLAN", "after main is healthy: restart marahome@%s, prove /login on port %s" % (_slug, _port or "?"))
else:
    for _slug, _port in agents_active:
        sh(["systemctl", "restart", AGENT + _slug])
        time.sleep(2)
        _ok = ro(["systemctl", "is-active", "--quiet", AGENT + _slug]).returncode == 0
        _code = "?"
        if _port:
            try:
                with urllib.request.urlopen("http://127.0.0.1:%s/login" % _port, timeout=20) as _resp:
                    _code = str(_resp.status)
            except Exception as _e:
                _code = type(_e).__name__
        agents_report["restarted"].append({"slug": _slug, "port": _port, "active": _ok, "login": _code})
        say("agents", "marahome@%s active=%s /login=%s" % (_slug, _ok, _code))
        if not _ok or (_code != "?" and _code != "200"):
            say("MANUAL-TODO", "agent %s NOT proven after restart: journalctl -u marahome@%s -n 40" % (_slug, _slug))

# ---------------- marker ----------------
marker = {
    "migrated_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "unit": UNIT, "unit_sha_after": sha16(frag) if not DRY else "",
    "daemon_sha": sha16(cairn_py) if (not DRY and os.path.exists(cairn_py)) else "",
    "backup": BACKUP_DIR, "caddy": caddy_state, "health": health,
    "data_moved": data_moved, "etc_moved": etc_moved, "code_renamed": rename_code,
    "agents": agents_report,
    "caddy_header": "rewritten (--caddy-header)" if CADDY_HEADER else "kept X-Mara-Slug (opt-in flag not passed)",
    "install_src": (INSTALL_SRC + " " + sha16(INSTALL_SRC)) if INSTALL_SRC else "copied live bytes",
}
if not DRY:
    os.makedirs("/etc/cairn", exist_ok=True)
    atomic_write("/etc/cairn/migrate.done", json.dumps(marker, indent=2), mode=0o600)

say("REPORT", json.dumps(marker))
say("done", "backup set: " + BACKUP_DIR)
sys.exit(0)