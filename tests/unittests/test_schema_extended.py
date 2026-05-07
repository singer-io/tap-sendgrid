from types import SimpleNamespace
from unittest.mock import MagicMock

from tap_sendgrid import schema as schema_module


class ParentStream:
    replication_method = "FULL_TABLE"
    replication_keys = tuple()
    key_properties = ("id",)

    def __init__(self, _client, _catalog):
        self.children = ["child_stream"]
        self.child_to_sync = []

    def is_selected(self):
        return True

    def write_schema(self):
        return None


class ChildStream(ParentStream):
    def __init__(self, _client, _catalog):
        self.children = []
        self.child_to_sync = []


def test_write_schema_collects_child_streams():
    parent = ParentStream(None, None)
    catalog = MagicMock()
    catalog.get_stream.return_value = SimpleNamespace(schema=SimpleNamespace(to_dict=lambda: {}), metadata=[])

    original_streams = schema_module.STREAMS
    schema_module.STREAMS = {"child_stream": ChildStream}
    try:
        schema_module.setup_stream_schema(parent, client=MagicMock(), catalog=catalog)
        assert len(parent.child_to_sync) == 1
    finally:
        schema_module.STREAMS = original_streams
