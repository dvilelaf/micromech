"""TDD RED: ChainConfig must NOT persist service_id, service_key, or multisig_address.

These fields belong to iwa's olas plugin and should not be duplicated in micromech config.
All tests here should FAIL on the current code and PASS after the fix.
"""

from micromech.core.config import ChainConfig, MicromechConfig

# Valid Ethereum addresses for test fixtures
MARKETPLACE = "0x735FAAb1c4Ec41128c367AFb5c3baC73509f70bB"
FACTORY = "0x8b299c20F87e3fcBfF0e1B86dC0acC06AB6993EF"
STAKING = "0xCAbD0C941E54147D40644CF7DA7e36d70DF46f44"
MECH = "0x1234567890abcdef1234567890abcdef12345678"


def _make_chain(**overrides) -> ChainConfig:
    """Helper to build a ChainConfig with required addresses."""
    defaults = dict(
        chain="gnosis",
        marketplace_address=MARKETPLACE,
        factory_address=FACTORY,
        staking_address=STAKING,
    )
    defaults.update(overrides)
    return ChainConfig(**defaults)


# --- Tests 1-3: fields must NOT exist ---


def test_chain_config_no_service_id():
    """ChainConfig must not persist service_id -- it belongs to iwa's olas plugin."""
    cfg = _make_chain()
    dumped = cfg.model_dump()
    assert "service_id" not in dumped


def test_chain_config_no_service_key():
    """ChainConfig must not persist service_key -- it belongs to iwa's olas plugin."""
    cfg = _make_chain()
    dumped = cfg.model_dump()
    assert "service_key" not in dumped


def test_chain_config_no_multisig():
    """ChainConfig must not persist multisig_address -- it belongs to iwa's olas plugin."""
    cfg = _make_chain()
    dumped = cfg.model_dump()
    assert "multisig_address" not in dumped


# --- Test 4: mech_address must remain ---


def test_chain_config_keeps_mech_address():
    """mech_address is mech-specific and must stay in ChainConfig."""
    cfg = _make_chain(mech_address=MECH)
    assert cfg.mech_address == MECH
    dumped = cfg.model_dump()
    assert "mech_address" in dumped


# --- Test 5: save/dump must not include service fields ---


def test_save_does_not_write_service_fields():
    """Saving config must not persist service_id/service_key/multisig_address."""
    cfg = MicromechConfig()
    data = cfg.model_dump(mode="json")
    gnosis = data.get("chains", {}).get("gnosis", {})
    assert "service_id" not in gnosis
    assert "service_key" not in gnosis
    assert "multisig_address" not in gnosis


# --- Test 6: setup_complete uses mech_address, not service_id ---


def test_setup_complete_uses_mech_address():
    """setup_complete should check mech_address, not service_id."""
    cc = _make_chain(mech_address=MECH)
    assert cc.setup_complete is True

    cc2 = _make_chain()
    assert cc2.setup_complete is False


# --- Test 7: detect_setup_state without service fields ---


def test_detect_setup_state_without_service_fields():
    """detect_setup_state should only care about mech_address."""
    cc = _make_chain(mech_address=MECH)
    assert cc.detect_setup_state() == "complete"

    cc2 = _make_chain()
    assert cc2.detect_setup_state() == "needs_create"
