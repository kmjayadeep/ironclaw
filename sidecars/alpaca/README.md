# Alpaca sidecar

Wraps Ledger's Alpaca `CoinModuleApi` as a local service the IronClaw Rust
backend calls (attested-signing Phase E).

## What it is authoritative for: nothing

| Decision | Authority | This sidecar |
|---|---|---|
| Canonical signing bytes, render, `ApprovedTxHash` | **Rust** (`ironclaw_attestation`) | proposes a crafted tx; if Rust cannot decode it, the raise fails closed |
| Signer identity | **Rust** (gate-bound `SigningContext`) | never consulted |
| Grant claim / one-shot | **Rust** (sealed-grant CAS) | never consulted |
| Bytes handed to `combine` | **Rust**, reconstructed from the binding | mechanically attaches a signature |
| Broadcast admission | **Rust** idempotency ledger CAS | executes the RPC submit |
| Fees, balances, chain height | sidecar | advisory only |

A compromised sidecar can propose a malicious transaction — and the human
clear-signing the *Rust-derived* render on the device is what catches it. It
holds no keys, sees no grants, and cannot alter the bytes the device signs.

## Transport

Unix domain socket in a `0700` directory, plus a per-boot token the Rust parent
generates and passes on stdin. Every request must carry the token. No inbound
port on any external interface, ever. Localhost TCP is the Windows/dev fallback
with the same token requirement.

## Running it

The Rust parent spawns and supervises it. Standalone, for development:

```sh
ALPACA_SOCKET_PATH=/tmp/alpaca.sock node --experimental-strip-types src/server.ts <<< "$TOKEN"
```
