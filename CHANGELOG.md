# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-03-09

### Added
- **Hierarchical Actor System**: Implemented robust parent-child relationships for reliable actor lifecycle management and name uniqueness within the `/user/` namespace.
- **Actor Supervision**: Introduced `OneForOneStrategy` with support for `Restart`, `Resume`, `Stop`, and `Escalate` directives to handle child actor failures.
- **Pekko Remote Support**: Added direct messaging compatibility for `pekko://` and `akka://` URIs via `ActorSelection`, enabling communication without mandatory cluster membership.
- **Actor-Aware Logging**: Integrated structured logging with `a.Log()` (via `log/slog`) that automatically includes actor paths, system names, and sender context.
- **Advanced Serialization**: Built-in support for **Protobuf** (ID 2), **Raw Bytes** (ID 4), and **JSON/Jackson-compatible** (ID 9) serialization.
- **GitHub Actions CI**: Automated CI pipeline for Go 1.24, including unit testing, coverage reporting, and linting.

### Changed
- **Config Engine Migration**: Replaced `go-akka/configuration` with the high-performance [`gekka-config`](https://github.com/sopranoworks/gekka-config) engine.
- **Location Transparency**: Refactored `ActorSelection` and `ArteryTransport` to support seamless addressing across local and remote nodes.
- **Namespace Enforcement**: `ActorOf` now strictly enforces the `/user/` path for all user-created actors.

### Fixed
- **Handshake Stability**: Improved Artery association handshake and termination signal delivery for more reliable remote connections.
- **Concurrency Fixes**: Resolved various race conditions in actor spawning and lifecycle transition hooks.
- **Message Dispatch**: Fixed a critical bug where messages were not correctly routed to registered actors by default when incoming envelopes contained full URIs.


[0.3.0]: https://github.com/sopranoworks/gekka/releases/tag/v0.3.0
