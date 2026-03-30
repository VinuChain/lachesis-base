# lachesis-base (VinuChain fork)

VinuChain's fork of [Fantom's lachesis-base](https://github.com/Fantom-foundation/lachesis-base), the core library implementing the Lachesis asynchronous DAG-based BFT consensus protocol.

This library defines the interfaces, data structures, and modules that power consensus in [VinuChain](https://github.com/VinuChain/VinuChain). It is not a standalone binary — it is imported as a Go module by the VinuChain node.

## Why the fork?

The upstream lachesis-base had several issues that required fixes not accepted or applicable upstream:

### P2P stream session hardening
- **Global session cap** — added `MaxGlobalSessions` to the stream seeder config, preventing a single peer (or many peers) from exhausting server resources by opening unbounded sessions.
- **Eliminated busy-wait spin loop** — replaced a `time.Sleep(10ms)` polling loop in `waitPendingResponsesBelowLimit` with a proper channel-based signal (`pendingSizeReduced`), making the seeder responsive to peer disconnects and shutdown signals instead of spinning.
- **Extracted peer unregister logic** — deduplicated session cleanup into `handleUnregister` for use in both the main reader loop and the backpressure wait path.

### KV store fixes
- **LevelDB cache curve adjustment** — tuned the `adjustCache` piecewise function to better match VinuChain's workload profile, allocating more cache at mid-range memory budgets.
- **Pebble compatibility** — changed `MaxConcurrentCompactions` from a closure to a plain integer to match the API expected by the Pebble version used in VinuChain's dependency tree.
- **Table prefix increment fix** — fixed `incPrefix` to correctly handle prefixes with leading zero bytes by trimming before computing the increment, preventing iterator range miscalculation.
- **Stat format consistency** — standardized `disk.size` output to `Size(B):N` format across both LevelDB and Pebble backends.

### Consensus test correction
- **Election test fix** — corrected a decisive roots assertion in the election test to match actual BFT quorum behavior, and extended the test DAG with additional frames for better coverage.

### Dependency alignment
- Pinned Go and dependency versions to match VinuChain's module graph (`go 1.14`, older Pebble, LevelDB, and testify versions), avoiding diamond dependency conflicts.

## Package structure

| Package | Purpose |
|---------|---------|
| `abft/` | Asynchronous BFT consensus: event processing, frame decision, Atropos election, epoch state |
| `abft/election/` | Byzantine agreement election algorithm |
| `abft/dagidx/` | DAG indexer for consensus |
| `emitter/` | Event emission scheduling and interval management |
| `eventcheck/` | Event validation rules (basic, epoch, parents) |
| `gossip/` | P2P gossip protocol: stream seeder/leecher for syncing events and blocks |
| `hash/` | Hash types and utilities |
| `inter/` | Core data types: events, blocks, timestamps, DAG structures |
| `kvdb/` | Key-value database abstraction with LevelDB, Pebble, and in-memory backends |
| `lachesis/` | Top-level Lachesis configuration |
| `utils/` | Shared utilities: caching, workers, piecewise functions, weighted sampling |
| `vecengine/` | Vector clock engine for DAG-based consensus |
| `vecfc/` | Forkless causality vector implementation |

## Usage

This module is consumed via a `replace` directive in VinuChain's `go.mod`:

```
replace github.com/Fantom-foundation/lachesis-base => github.com/VinuChain/lachesis-base v0.1.0-elemont
```

The module path remains `github.com/Fantom-foundation/lachesis-base` for compatibility with existing imports throughout the VinuChain codebase (inherited from the upstream go-opera fork).

### Building and testing

```shell
go test ./...
```

There is no standalone binary. All packages are library code imported by VinuChain.

## Versioning

| Tag | Description |
|-----|-------------|
| `v0.1.0-elemont` | Initial VinuChain fork with stream hardening, KV store fixes, and dependency alignment |

## License

GNU Lesser General Public License v3.0 — see [COPYING](COPYING) and [COPYING.LESSER](COPYING.LESSER).
