# Releasing SymFynity

## Licence checks — read before every release

SymFynity is licensed under the Business Source License 1.1 (see [`LICENSE`](LICENSE)).
BSL has a small number of ways to go quietly wrong at release time. None of them
error; they all just silently give away more than intended.

### Do not replace the relative Change Date with a fixed date

The licence says:

```
Change Date:          Four years from the date the Licensed Work is published.
```

This is deliberate and must stay relative. BSL applies **separately to each
version**, and each version's clock starts when *that version* is published — so
the relative form means every release automatically gets its own correct
four-year date, with no upkeep.

Substituting a fixed date (`Change Date: 2030-07-15`) looks tidier and is a trap.
That date would then apply to *every* subsequent release, so a version shipped in
2029 would convert to Apache-2.0 after one year instead of four. The error is
invisible until it has already happened, and it cannot be undone — a version
published under a given Change Date keeps it forever.

The four-year cap is enforced by the licence text regardless (`or the fourth
anniversary of the first publicly available distribution ... whichever comes
first`), so the relative form can never grant *less* than intended, only exactly
what's intended.

### Do not bump `Licensed Work` per release

```
Licensed Work:        SymFynity Version 0.3.0 or later.
```

The `or later` covers all future versions. This line changes only if the
licensing policy itself changes — e.g. a future version moves to different terms,
or the Licensed Work is renamed. It is not a version-bump chore.

It has changed exactly once, at 0.3.0, when the proxy was renamed from Weir to
SymFynity. That is the bar: a change of identity or terms, not a release.

### Bump the copyright year in January

```
Licensed Work:        SymFynity Version 0.3.0 or later. The Licensed Work is
                      (c) 2026 SYMFYNITY LIMITED.
```

Stale years are cosmetic, not fatal — but they are the first thing a reviewing
solicitor notices.

### Confirm the LICENSE ships with every artifact

BSL: *"You must conspicuously display this License on each original or modified
copy of the Licensed Work."* A published binary or image is a copy.

- Docker image — `Dockerfile` copies `LICENSE` to `/usr/local/share/symfynity/LICENSE`.
  Verify after any Dockerfile restructure:
  ```bash
  docker run --rm --entrypoint cat <image> /usr/local/share/symfynity/LICENSE | head -3
  ```
- Any new distribution channel (tarball, crates.io, package repo) needs the same
  check before it is used for the first time.

### Check new dependency licences

SymFynity is distributed as a compiled binary, so a copyleft dependency pulled into
the tree becomes a licensing problem for the whole artifact — BSL does not
override an upstream GPL obligation. Worth a look whenever `Cargo.lock` gains
entries:

```bash
cargo tree --format '{p} {l}' | grep -viE 'MIT|Apache-2.0|BSD|ISC|Unicode|Zlib' | sort -u
```

## Version history and licensing

| Version | Published as | Licence |
|---|---|---|
| 0.1.0 | Weir | Apache License 2.0 |
| 0.2.0 | Weir | Business Source License 1.1 |
| 0.3.0 onward | SymFynity | Business Source License 1.1 → Apache-2.0 after four years |

Both earlier versions were published publicly under the name Weir, and 0.1.0 under
Apache-2.0. Those terms are irrevocable for that version and anyone who obtained
it — that is expected and fine, not a leak to be plugged. The licence line sits
between 0.1.0 and 0.2.0; the name line sits between 0.2.0 and 0.3.0.

Neither is worth rewriting history over. A published version keeps the name and
the terms it was published under, permanently, and any record that says otherwise
is simply wrong rather than helpfully tidy.

## If the licence is ever changed again

Publishing is one-way. Any version already distributed keeps the terms it was
distributed under, permanently. A future relicence therefore applies only to
*subsequent* versions, and the practical cost of one rises with adoption:
contributors gain copyright in their contributions, so once external PRs are
merged, relicensing needs either a CLA on file or every contributor's agreement.

While SYMFYNITY LIMITED is the sole copyright holder, relicensing is a
one-file change. That stops being true with the first external contribution —
so if a CLA is wanted, it needs to be in place *before* contributions are
accepted, not after.
