# Contributing to the Kesh core fork

The branch model, commit style and release process for all four fork repositories are in the repeater's [CONTRIBUTING.md](https://github.com/kesh33w/openhop_repeater/blob/kesh/bridge/CONTRIBUTING.md). Points specific to core:

- **Gate:** `pre-commit run --all-files` runs black, isort, flake8 and the full test suite.
  - black and isort skip `tests/`, but flake8 does not, so keep test files flake8-clean, with no star imports.
- **Python 3.10 is supported.** Avoid `asyncio.timeout` and `set_authorizer(None)` in tests; on 3.10, `concurrent.futures.TimeoutError` is not the built-in `TimeoutError`.
- **The repeater depends on this branch** through `openhop_core@kesh/bridge`. Merge core changes first, then the repeater changes that use them. Keep public names that the repeater imports stable, or change both in coordinated PRs.
- **Hardware drivers:**
  - A send must report one of these outcomes:
    - confirmed;
    - definitely not sent (`ok: False`, `tx_outcome_uncertain: False`);
    - uncertain (`None`).
  - Never report a frame that may have reached the modem as definitely not sent.
- **Upstream sync:** merge `openhop-dev/openhop_core` `dev` into a `sync/upstream-YYYYMMDD` branch, never rebase, then open a PR. Hotspots: `node/dispatcher.py`, `hardware/usb_radio.py`, `hardware/tcp_radio.py` and `rf_fabric/fabric.py`.
