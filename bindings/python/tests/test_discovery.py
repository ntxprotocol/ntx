"""mDNS discovery of a real advertised source."""

from __future__ import annotations

import uuid

import ntxprotocol as ntx
from conftest import TIMEOUT, expect


def test_browser_finds_and_loses_a_source() -> None:
    source_id = f"python-test-{uuid.uuid4()}"
    with ntx.Browser() as browser:
        source = ntx.Source("Python Discovery Test", id=source_id, port=0, pairing_code="1234")
        found = expect(browser, ntx.SourceFound, timeout=3 * TIMEOUT)
        while found.source.id != source_id:
            found = expect(browser, ntx.SourceFound, timeout=3 * TIMEOUT)
        assert found.source.port == source.port
        assert found.source.url.endswith(f":{source.port}/")
        assert found.source.version == ntx.PROTOCOL_VERSION
        assert found.source.pairing_required
        assert found.source.addresses

        source.close()
        lost = expect(browser, ntx.SourceLost, timeout=3 * TIMEOUT)
        while lost.id != source_id:
            lost = expect(browser, ntx.SourceLost, timeout=3 * TIMEOUT)
        assert lost.name == found.source.name
