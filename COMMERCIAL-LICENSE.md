# Commercial licensing

CAIRN is dual-licensed, the way Qt and MySQL do it:

- **Community:** GNU AGPL-3.0-or-later (the text in `LICENSE`). That file is
  the actual license grant. This page is a plain-language guide, not a
  contract, and it cannot widen or narrow the AGPL.
- **Commercial:** for uses where you cannot or do not want to comply with
  the AGPL — most commonly, building a **closed-source** product or service
  on CAIRN.

## What you can do for free (under the AGPL)

- Run CAIRN for yourself, your family, your friends, your club.
- Run it for your company, internally.
- Offer it as a hosted service **to the public** — as long as you provide
  the corresponding source of your version (including any modifications)
  to everyone you serve, under the AGPL.
- Fork it, study it, tear it apart, ship your own AGPL fork.

The AGPL was chosen deliberately: an AI assistant daemon is *used over a
network*, and the ordinary GPL would let someone host it as a closed
service. The AGPL closes that hole. Your freedom to use the software is
real; using it as a moat for a closed derivative is what costs money.

## When you need a commercial license

- You want to embed or ship CAIRN (or a derived daemon) inside a product
  whose source you cannot release under the AGPL.
- You want the right to use the CAIRN name/brand beyond attribution.
- You want support terms, an SLA, or an indemnity conversation.

## How to get one

Open an issue you'd be comfortable making public, or use the contact
listed on [github.com/K80-DEV](https://github.com/K80-DEV). Say what you
want to build and how CAIRN fits in it. Pricing scales with what you're
doing it for — a hobby wrapper and a funded product are different
conversations, and we'd rather have both than neither.

## What a commercial license will never be

- A claim on your data, your keys, or your models. CAIRN is BYOK; that
  doesn't change under any license.
- A backdoor or a phone-home. The update check stays opt-in and the
  telemetry stays at zero in every licensed build, because those properties
  live in the code, and the code is the same code.

Copyright (c) 2026 K80.DEV.
