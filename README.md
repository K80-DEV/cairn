# CAIRN

Your own self-hosted AI home. A single-file Python daemon that gives you a
personal, private, multi-user chat assistant with your own model keys (BYOK),
memory, scheduled tasks, media generation, file sharing, and a warm goblin
keeping the lights on.

> ## ⚠️ PRE-RELEASE — NOT YET FULLY SECURITY AUDITED
> This build predates completion of the external security-audit process.
> It has been through nine documented audit rounds (P1-A … P1-H) and every
> finding to date is fixed and regression-tested — **but the audit is not
> finished.** Do not expose an instance to the public internet yet. Run it
> on your LAN (or behind a tunnel you control), pick strong passwords, and
> understand that the assistant's tools execute with real local privileges.
> What this is not: it is not telemetry-free-by-hope — it ships with **zero
> outbound calls** except the model API *you* configure and an update check
> that is **opt-in and off by default**.

## What you get

- **Web chat UI** with streaming, attachments, per-chat model switching
- **BYOK**: any OpenAI-compatible endpoint (OpenAI, OpenRouter, Featherless,
  Ollama on your own box, …). Keys are write-only, never echoed, never logged.
- **Multi-user with tiers** (owner/admin/user) and per-user memory isolation
- **First-boot setup wizard** with an honest, un-skippable explanation of
  what an AI with local tools can do — you accept it with eyes open or you
  don't run it
- **Scheduled tasks** (5-field cron, per-user timezones)
- **File send/receive in chat**, OCR + audio transcription (tesseract /
  whisper.cpp), image generation (Cloudflare Workers AI / OpenAI-compatible)
- **Vault**: secrets sealed with scrypt+AES-256-GCM; optional external key
  server for the master key
- **Full encrypted backup/export/import** (your password, max reasonable KDF)
- **Pull-updater**: signed manifest (Ed25519), stage-before-swap,
  gold-boot smoke test, automatic pre-update backup — disabled by default
- No telemetry. No accounts. No cloud required. The only door is yours.

## Quick start (Linux)

```bash
sudo apt install python3 python3-cryptography tesseract-ocr ffmpeg
python3 marahome.py           # starts on http://0.0.0.0:8470
# open http://<your-box>:8470 and follow the first-boot wizard
```

> **First boot: open it on the machine itself (`http://localhost:8470`) or
> over HTTPS.** Session cookies carry the `Secure` flag, so browsers keep
> them only on HTTPS connections or on `localhost`. Loading the app over a
> plain-HTTP LAN address (e.g. `http://192.0.2.x:8470`) works, but logins
> will not stick on that transport. Known limitation, fix planned before
> 1.0; meanwhile either browse on the box itself, or put the daemon behind
> any TLS terminator you control (a Cloudflare Tunnel, or a two-line Caddy
> `tls internal`, both do it). Plain HTTP from another device is exactly
> the transport we are refusing to hand a session token over.

A `.deb` (with systemd unit + unprivileged service user) is the target
install path for 1.0; manual run above already works on any box with
Python 3.11+.

## Update channel

Updates are pulled (never pushed), verified against a pinned Ed25519 key,
and only applied when you click Install. Manifests and files will live in
this repository's tree (raw.githubusercontent.com serves them with no
redirects — which our updater requires by design).

## License

Copyright (c) 2026 K80.DEV

CAIRN is licensed under the **GNU AGPL-3.0-or-later** (see `LICENSE`).

- Free to use, fork, and modify — including running it as a network service
  for others, as long as you keep the source open under the same license.
- Want to build a **closed-source** product on it? That requires a
  commercial license — see [`COMMERCIAL-LICENSE.md`](COMMERCIAL-LICENSE.md)
  or use the contact listed on [github.com/K80-DEV](https://github.com/K80-DEV).

## Built with AI assistance

CAIRN was designed and built by **K80.DEV with substantial AI assistance**
(the daemon's own resident goblin, Mara, wrote much of this codebase and all
of its regression harnesses). The audit trails in the comments are real.

## Disclaimer

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. Running an autonomous
assistant with local tool access is YOUR operational decision — see the
risk acceptance screen in the first-boot wizard, and mean it.
