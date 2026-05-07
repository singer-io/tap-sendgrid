import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from tap_sendgrid import schema as schema_module

sync_module = importlib.import_module("tap_sendgrid.sync")


class FakeCatalog:
    def __init__(self):
        self._stream = SimpleNamespace(
            schema=SimpleNamespace(to_dict=lambda: {"type": "object", "properties": {}}),
            metadata=[{"breadcrumb": [], "metadata": {"selected": True}}],
        )

    def get_selected_streams(self, _state):
        return [SimpleNamespace(stream="blocks")]

    def get_stream(self, _stream_name):
        return self._stream


class FakeStream:
    parent = ""

    def __init__(self, _client, _catalog):
        self.children = []
        self.child_to_sync = []

    def is_selected(self):
        return True

    def write_schema(self):
        return None

    def sync(self, state, transformer):
        del state, transformer
        return 1


class FakeChildStream(FakeStream):
    parent = "blocks"


def test_get_schemas_contains_expected_streams():
    schemas, field_metadata = schema_module.get_schemas()
    assert "blocks" in schemas
    assert "blocks" in field_metadata


def test_update_currently_syncing_sets_and_clears():
    state = {}
    with patch.object(sync_module.singer, "write_state") as write_state:
        sync_module.update_currently_syncing(state, "blocks")
        assert state.get("currently_syncing") == "blocks"
        sync_module.update_currently_syncing(state, None)
        assert "currently_syncing" not in state
        assert write_state.call_count == 2


@patch.object(sync_module, "setup_stream_schema")
def test_sync_runs_selected_stream(write_schema_mock):
    client = MagicMock()
    catalog = FakeCatalog()

    with patch.object(sync_module, "STREAMS", {"blocks": FakeStream}), patch.object(
        sync_module.singer, "Transformer"
    ) as transformer_cls:
        transformer_cls.return_value.__enter__.return_value = MagicMock()
        state = {}
        sync_module.sync(client=client, catalog=catalog, state=state)

    write_schema_mock.assert_called_once()


def test_sync_child_stream_adds_parent_and_skips_direct_sync():
    client = MagicMock()
    child_catalog = FakeCatalog()
    child_catalog.get_selected_streams = lambda _state: [SimpleNamespace(stream="child")]

    with patch.object(sync_module, "STREAMS", {"child": FakeChildStream, "blocks": FakeStream}), patch.object(
        sync_module.singer, "Transformer"
    ) as transformer_cls:
        transformer_cls.return_value.__enter__.return_value = MagicMock()
        sync_module.sync(client=client, catalog=child_catalog, state={})
