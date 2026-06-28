# Persistent Node Identity

`xprv serve` (and any binary using `peer-client`) speaks the XRPL/Xahau peer
HTTP-Upgrade handshake with a secp256k1 keypair — the `Public-Key` header and
`Session-Signature` over the TLS Finished bytes. Without persistence, each
process start mints a fresh keypair and every restart joins the network as a
new "node", which network crawlers record as a single-sighting "phantom" peer.

Three ways to provide a stable identity, highest precedence first:

## 1. `CATL_NODE_SEED` env var (recommended for Cloud Run / K8s)

A literal base58 `NODE_PRIVATE` seed string (XRPL convention — starts with `p`,
51 chars). Injected directly into the process; no filesystem required.

```bash
export CATL_NODE_SEED="$(xprv gen-node-seed)"
xprv serve
```

For Cloud Run this is wired automatically by `.ai-docs/scripts/deploy.sh`:
the script creates a Secret Manager secret on first deploy and mounts it as
`CATL_NODE_SEED` via `--set-secrets`. Subsequent deploys reuse the same secret.

### Rotation and the `:latest` contract

> **Working assumption (2026-06-28, unverified empirically):**
> Cloud Run resolves `--set-secrets=…:latest` at *revision-create time*
> (when `gcloud run deploy` runs), pins the resolved version for that
> revision's lifetime, and does NOT re-resolve when an instance restarts
> or scales back from zero.
>
> **Operational consequence:** rotating the seed requires both
> `gcloud secrets versions add …` **and** `gcloud run deploy` afterwards.
>
> **How to validate (do this once before relying on it):**
> 1. Note the `Node identity → public key n…` line on revision N.
> 2. `gcloud secrets versions add xprv-node-seed --data-file=new.seed`
> 3. Force a cold start (set `min-instances=0` then back to `1`).
> 4. Read the public-key log on revision N's fresh instance.
>    - If unchanged → assumption holds.
>    - If changed → revision N is silently re-resolving `:latest`;
>      our rotation playbook is wrong. Update both this doc and
>      `.ai-docs/scripts/deploy.sh`.
>
> See the `OPEN DOUBTS` block in `.ai-docs/scripts/deploy.sh` for
> related scenarios that haven't been validated.

## 2. `--node-credentials <path>` / `CATL_NODE_CREDENTIALS` (Compute Engine VM / Docker volume)

A filesystem path to a 32-byte raw secret-key file. Read if it exists;
generated and atomically written on first run if missing and the parent
directory is writable. Mode `0600`.

```bash
# Compute Engine VM:
mkdir -p /var/lib/xprv
xprv serve --node-credentials /var/lib/xprv/node.seed

# Docker:
docker run -v xprv-data:/var/lib/xprv \
  -e CATL_NODE_CREDENTIALS=/var/lib/xprv/node.seed \
  catl/xprv serve
```

## 3. Default `$HOME/.peermon` (local dev)

If neither of the above is set, `xprv` falls back to `$HOME/.peermon` — the
same convention used by `lesser-peer` / `peermon`. The file is read if
present, generated-and-written on first run otherwise. So local dev gets
stable identity automatically after the first run.

## Generating a seed standalone

```bash
xprv gen-node-seed
# → pa35yFbx2u7aiPzmKRo4cjZBvbLE8Qa9qgfomFLZiJzEPWcsfvA
```

The output is a base58 `NODE_PRIVATE` string suitable for `CATL_NODE_SEED`,
or for storing in any secret manager. The seed is consumed by
`crypto_utils::node_keys_from_private` round-tripping through the same
encoding that lesser-peer's manual base58 keys use.

## Verifying identity is stable

At startup `xprv serve` logs a line of the form:

```
[xprv] Node identity: CATL_NODE_SEED (env-injected) → public key n9MzGS…
```

The `public key n…` value (base58 `NODE_PUBLIC`) should be identical across
restarts. If it changes, persistence is broken — confirm the env var or
credentials path is reaching the container.

## Conflict handling

If both `CATL_NODE_SEED` and `CATL_NODE_CREDENTIALS` are set, `xprv` prefers
`CATL_NODE_SEED` and logs a warning. This is intentional: the env var is the
narrower, more deliberate signal (typically Secret Manager), and ignoring it
would silently fall back to whatever the filesystem decides.

## Why this is not the xahaud / rippled seed format

`xahaud` and `rippled` use a 16-byte family seed (`s…` prefix). `xprv` /
`peer-client` use the 32-byte raw secret encoded with the `NODE_PRIVATE`
version prefix (`p…`) — same format `lesser-peer` and `peermon` have used
for years. The keypair is functionally equivalent for the peer handshake
(secp256k1 either way), but the wire format is not interchangeable with
`xahaud`'s seed files. A follow-up may add a `--seed-format=family` flag
for cross-tool compat.
