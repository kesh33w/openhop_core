# Changelog

Fork-specific changes to OpenHop Core. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Releases are tagged `kesh-YYYY.MM.DD`, and the same tag is used across the repeater, core, web UI and hub repositories.

## [Unreleased]

## [kesh-2026.11.19] - 2026-11-19

### Changed

- **README** points feedback and bug reports at the hub's issues, the single place for all four repositories.

## [kesh-2026.11.18.2] - 2026-11-19

### Changed

- No library changes. Tagged so all four repositories share the release name.

## [kesh-2026.11.18.1] - 2026-11-18

### Fixed

- **PyPI publish workflow:** upstream's workflow no longer runs in this fork. Publishing the `kesh-2026.11.18` GitHub Release triggered it; it ran tests and a build, then PyPI rejected the upload. Nothing was published.

## [kesh-2026.11.18] - 2026-11-18

This is the first tagged fork release. It covers everything since the fork's upstream base, `openhop-dev/openhop_core` `dev` at `141d944` (2026-10-19).

### Added

- **Network topology** (`rf_fabric/topology.py`):
  - two named logical networks, each with one designated transmitter, plus `receiver` and `shadow` radios;
  - network-aware forwarding and TX policy checks.
- **Logical receivers**: receptions from several radios on one network combine into a single reception that keeps bounded provenance (which radios heard it).
- **Ordered TX groups**:
  - task-local contexts that send one logical packet through two radios on the same network, in order;
  - declared final-hop variants, used by antenna identities;
  - `tx_group_scopes` to apply a group to bridged (crossing) sends only.
- **Per-radio CAD on/off**, applied only after the modem acknowledges, for both USB and TCP modems.
- **Ordered raw receive receipts** for an optional bot fallback.
- **Reception capture** (`OPENHOP_RX_CAPTURE_PATH`) for comparing antennas.
- **Private-peer provenance**: Internet Link packets carry their origin and expiry without any change to the wire packet.
- **`is_connected`** on USB and TCP radios, so the fabric skips a transmitter that is disconnected.
- Fork README with a diagram, CHANGELOG, CONTRIBUTING and SECURITY.
- CI runs upstream's pre-commit gate on pushes to `kesh/bridge`.

### Changed

- **Transmit outcomes**:
  - Explicit radio outcomes are honoured, with no fallback retry through the default radio.
  - Suppressed destination duplicates are distinct from failed sends.
- **CAD**: a missing or invalid CAD reply no longer drops the packet; it is sent as if CAD were off. The USB session reconnects only after three CAD timeouts in a row.
- **USB I/O**: serial reads and writes run on isolated threads, and stale modem sessions are fenced off.
- **Code style**: fork code follows upstream's black, isort and flake8 rules.

### Fixed

- **Signal readings**: per-packet USB RSSI and SNR are preserved instead of being overwritten by later packets.
- **Busy channel**: a definitive "channel busy" rejection from the modem stays safe to retry.
- **Duplicates**: a crossing that is re-flooded on its destination network was forwarded back to its source network (`destination_forwarding`).
- **Unsent sends**: sends that never reached the modem were reported as "uncertain", which blocked the retry for 300 s.
- **USB begin**: `begin()` failed spuriously straight after `cleanup()`.
- **USB command lock**: it could leak on Python 3.10/3.11 when a waiting command was cancelled.
- **Reception capture**: reconfiguring the topology leaked the previous capture thread, and refreshed TX entries stalled expiry.

[Unreleased]: https://github.com/kesh33w/openhop_core/compare/kesh-2026.11.18.2...kesh/bridge
[kesh-2026.11.18.2]: https://github.com/kesh33w/openhop_core/compare/kesh-2026.11.18.1...kesh-2026.11.18.2
[kesh-2026.11.18.1]: https://github.com/kesh33w/openhop_core/compare/kesh-2026.11.18...kesh-2026.11.18.1
[kesh-2026.11.18]: https://github.com/kesh33w/openhop_core/compare/141d944...kesh-2026.11.18
