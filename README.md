# Kesh's OpenHop Core bridge fork

This is Kesh's fork of [OpenHop Core](https://github.com/openhop-dev/openhop_core), the Python MeshCore protocol and radio library. The `kesh/bridge` branch supplies radio handling for [Kesh's OpenHop bridge](https://github.com/kesh33w/kesh-openhop-bridge), a Raspberry Pi repeater with two radios on different frequencies.

Core provides the radio and packet interfaces. The [Repeater fork](https://github.com/kesh33w/openhop_repeater/tree/kesh/bridge) implements channel translation, routing policy and queues. The hub contains the optional iLink sidecar and enrollment service.

## Changes used by the bridge

- The [RF fabric](src/openhop_core/rf_fabric) records the receiving radio and supports sends through a named radio. The dispatcher still handles packet deduplication across radios.
- The [dispatcher](src/openhop_core/node/dispatcher.py) passes receive-radio metadata to compatible raw-packet subscribers and packet handlers.
- Send results distinguish a failed transmission from an uncertain outcome. An internal driver error does not trigger a second send through the default radio.
- The [USB driver](src/openhop_core/hardware/usb_radio.py) captures each packet's RSSI and SNR before handing it to the event loop. A later packet cannot overwrite the earlier packet's signal readings. Status getters still report the most recent reception.

A successful modem transmission does not establish recipient delivery. The library also does not make cross-channel translation end-to-end encrypted. The Repeater must decrypt an eligible group message with its source key and encrypt a new copy with its destination key.

## Use matching source

For the full bridge, follow the hub's [reconstruction workflow](https://github.com/kesh33w/kesh-openhop-bridge#reconstruct-and-test). Select `components.current.json` explicitly. That manifest records a dated source snapshot, not necessarily the latest branch tips or the software installed on a Pi.

To work on this Core branch alone:

```sh
git clone --branch kesh/bridge --single-branch https://github.com/kesh33w/openhop_core.git
cd openhop_core
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[test]'
.venv/bin/python -m pytest --tb=short
```

The package requires Python 3.10 or later. The hub's recorded integration environment uses Python 3.14. These commands install this checkout in a virtual environment; `pip install openhop-core` installs the upstream package instead.

The `radio` extra adds serial support. The `hardware` extra adds SPI, GPIO and USB dependencies, some of which are platform-specific. Install only the extras required by your hardware. Running unit tests does not require a connected radio.

## Source and tests

| Path | Purpose |
| --- | --- |
| [src/openhop_core/rf_fabric](src/openhop_core/rf_fabric) | Multi-radio receive observations and transmit selection |
| [src/openhop_core/hardware](src/openhop_core/hardware) | SPI, USB and network radio drivers |
| [src/openhop_core/node](src/openhop_core/node) | Mesh node, dispatcher and handlers |
| [src/openhop_core/protocol](src/openhop_core/protocol) | Packet encoding and protocol definitions |
| [tests/test_usb_rx_metrics.py](tests/test_usb_rx_metrics.py) | Per-packet USB signal readings and callback compatibility |
| [tests/test_dispatcher_tx_outcome.py](tests/test_dispatcher_tx_outcome.py) | Failed and uncertain sends |
| [tests/test_dispatcher_raw_rx_radio.py](tests/test_dispatcher_raw_rx_radio.py) | Receive-radio metadata for raw subscribers |
| [docs/openhop-frame-extensions.md](docs/openhop-frame-extensions.md) | OpenHop-specific companion frames |

The [examples](examples) exercise the library directly. Review their radio settings and host requirements before running them on hardware. Unit-test results do not establish RF range, regulatory compliance or field reliability.

## Upstream and license

OpenHop Core supplies the underlying Python implementation. [MeshCore](https://github.com/meshcore-dev/MeshCore) supplies the original C++ protocol implementation. The [upstream documentation](https://docs.openhop.dev/projects/openhop-core/) covers general library use, but does not describe all changes on this branch.

The original copyright notices and [MIT License](LICENSE) remain unchanged. Report fork-specific problems in [this repository](https://github.com/kesh33w/openhop_core/issues), with the commit, radio backend and steps to reproduce. Keep channel keys and device credentials out of reports.
