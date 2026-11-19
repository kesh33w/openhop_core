import asyncio
import pytest
from openhop_core.rf_fabric.fabric import RFFabric
from openhop_core.rf_fabric.topology import NetworkTopology
from openhop_core.protocol import Packet
from openhop_core.protocol.constants import PAYLOAD_TYPE_ADVERT


def topology(order=None, extra_role="transceiver"):
    entries = [
        dict(id="smf", network="SMF", role="transceiver"),
        dict(id="bay", network="SFO", role="transceiver"),
        dict(id="rak", network="SFO", role=extra_role),
    ]
    settings = dict(networks={"SMF": "smf", "SFO": "bay"})
    if order is not None:
        settings["tx_groups"] = {"bay": order}
    return NetworkTopology(entries, settings, "smf")


class Radio:
    _cad_mode_explicit = False
    lbt_enabled = False

    def __init__(self, result=True):
        self.result, self.sent = result, []

    def set_rx_callback(self, cb):
        pass

    async def send(self, data):
        self.sent.append(data)
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


def setup(order=("rak", "bay"), result=True):
    fabric = RFFabric()
    radios = {rid: Radio(result) for rid in ("smf", "bay", "rak")}
    for rid, radio in radios.items():
        fabric.register_radio(radio, radio_id=rid)
    fabric.configure_topology(topology(list(order)))
    pkt = Packet()
    pkt.header = PAYLOAD_TYPE_ADVERT << 2
    pkt.payload = bytearray(b"ordered-test")
    pkt.payload_len = len(pkt.payload)
    return fabric, radios, pkt.write_to()


def test_default_preserves_rx_only_and_primary_identity():
    t = topology(extra_role="shadow")
    assert t.tx_targets("bay") == ("bay",)
    with pytest.raises(ValueError):
        t.check_tx("rak")


def test_ordered_groups_keep_logical_primary_and_policy():
    t = topology(["rak", "bay"])
    assert t.tx_targets("bay") == ("rak", "bay")
    assert t.home("rak") == "bay"
    assert t.reply_destination("rak") == "bay"
    with pytest.raises(ValueError):
        t.check_tx("rak")
    t.check_tx("rak", "smf", group_primary="bay")
    with pytest.raises(ValueError):
        t.check_tx("rak", reply_to_radio_id="smf", group_primary="bay")


@pytest.mark.parametrize(
    "order", [["rak", "rak", "bay"], ["smf", "bay"], ["rak"], [], ["missing", "bay"]]
)
def test_invalid_groups(order):
    with pytest.raises(ValueError):
        topology(order)


def test_extra_transceiver_requires_opt_in():
    with pytest.raises(ValueError):
        topology()


@pytest.mark.parametrize("role", ["shadow", "receiver"])
def test_group_secondary_keeps_receive_role_and_routing(role):
    entries = [
        dict(id="smf", network="SMF", role="transceiver"),
        dict(id="bay", network="SFO", role="transceiver"),
        dict(id="rak", network="SFO", role=role),
    ]
    base = {"networks": {"SMF": "smf", "SFO": "bay"}, "enable_receiver_forwarding": True}
    before = NetworkTopology(entries, base, "smf")
    after = NetworkTopology(entries, {**base, "tx_groups": {"bay": ["rak", "bay"]}}, "smf")
    assert before.members == after.members
    assert before.destinations("rak") == after.destinations("rak")
    assert before.relay_destinations("rak") == after.relay_destinations("rak")
    with pytest.raises(ValueError):
        after.check_tx("rak")
    after.check_tx("rak", "smf", group_primary="bay")


@pytest.mark.asyncio
async def test_both_legs_once_same_bytes_one_network_claim():
    f, radios, data = setup()
    with f.ordered_tx_group("bay"):
        assert (await f.send(data, radio_id="rak"))["ok"]
        assert (await f.send(data, radio_id="bay"))["ok"]
        with pytest.raises(ValueError):
            await f.send(data, radio_id="rak")
    assert radios["rak"].sent == radios["bay"].sent == [data]
    assert len(f._network_tx_seen) == 1
    assert (await f.send(data, radio_id="bay"))["suppressed"]
    with f.ordered_tx_group("bay"):
        assert (await f.send(data, radio_id="rak"))["suppressed"]
    assert len(radios["rak"].sent) == 1


@pytest.mark.asyncio
async def test_cad_setting_does_not_gate_or_change_ordered_group():
    f, radios, data = setup()
    radios["bay"]._cad_mode_explicit = None
    radios["rak"].lbt_enabled = True
    with f.ordered_tx_group("bay"):
        await f.send(data, radio_id="rak")
        await f.send(data, radio_id="bay")
    assert radios["rak"].sent == radios["bay"].sent == [data]
    assert radios["bay"]._cad_mode_explicit is None
    assert radios["rak"].lbt_enabled is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result", [None, {"ok": False, "tx_outcome_uncertain": True}, RuntimeError("lost response")]
)
async def test_uncertainty_blocks_next_leg_and_preserves_claim(result):
    f, radios, data = setup()
    radios["rak"].result = result
    with f.ordered_tx_group("bay") as group:
        try:
            await f.send(data, radio_id="rak")
        except RuntimeError:
            pass
        assert group.aborted
        with pytest.raises(ValueError):
            await f.send(data, radio_id="bay")
    assert not radios["bay"].sent
    assert len(f._network_tx_seen) == 1


@pytest.mark.asyncio
async def test_definite_failure_allows_second_and_all_failure_releases_claim():
    f, radios, data = setup(result=False)
    with f.ordered_tx_group("bay"):
        assert not await f.send(data, radio_id="rak")
        assert not await f.send(data, radio_id="bay")
    assert not f._network_tx_seen
    radios["bay"].result = True
    assert (await f.send(data, radio_id="bay"))["ok"]


@pytest.mark.asyncio
async def test_changed_bytes_and_scope_escape_rejected():
    f, radios, data = setup()
    release = asyncio.Event()

    async def late_send():
        await release.wait()
        with pytest.raises(ValueError, match="closed"):
            await f.send(data, radio_id="bay")

    with f.ordered_tx_group("bay"):
        await f.send(data, radio_id="rak")
        with pytest.raises(ValueError, match="identical"):
            await f.send(data + b"x", radio_id="bay")
        task = asyncio.create_task(late_send())
    release.set()
    await task
    assert not radios["bay"].sent


@pytest.mark.asyncio
async def test_definitely_unsent_leg_retry_preserves_cad():
    f, radios, data = setup()
    radios["rak"].result = {"ok": False, "tx_outcome_uncertain": False}
    with f.ordered_tx_group("bay"):
        assert not (await f.send(data, radio_id="rak"))["ok"]
        radios["rak"].result = True
        assert (await f.send(data, radio_id="rak"))["ok"]
        radios["bay"].lbt_enabled = True
        assert (await f.send(data, radio_id="bay"))["ok"]
    assert len(radios["rak"].sent) == 2
    assert radios["bay"].sent == [data]
    assert radios["bay"].lbt_enabled is True


@pytest.mark.asyncio
async def test_scope_blocks_overlap_and_expiry():
    f, radios, data = setup()
    started, release = asyncio.Event(), asyncio.Event()

    async def wait_send(data):
        started.set()
        await release.wait()
        return True

    radios["rak"].send = wait_send
    with f.ordered_tx_group("bay") as group:
        task = asyncio.create_task(f.send(data, radio_id="rak"))
        await started.wait()
        with pytest.raises(ValueError, match="in flight"):
            await f.send(data, radio_id="bay")
        release.set()
        await task
        group.deadline = 0
        with pytest.raises(ValueError, match="expired"):
            await f.send(data, radio_id="bay")
