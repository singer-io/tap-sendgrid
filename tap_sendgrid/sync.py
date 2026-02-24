"""Sync mode orchestration for tap-sendgrid.

Iterates registered streams in order, writes Singer SCHEMA messages, and
drives record extraction for each selected stream.
"""
import singer
from singer import Transformer

from tap_sendgrid.schema import write_schema
from tap_sendgrid.streams import STREAMS

LOGGER = singer.get_logger()


def update_currently_syncing(state, stream_name):
    """Set or clear the currently-syncing stream in *state* and emit a STATE message."""
    if stream_name:
        state["currently_syncing"] = stream_name
    else:
        state.pop("currently_syncing", None)
    singer.write_state(state)


def sync(client, catalog, state):
    """Sync all selected streams from the catalog."""
    with Transformer() as transformer:
        for stream_id, stream_class in STREAMS.items():
            # Child streams are driven by their parents — skip them here
            if getattr(stream_class, "parent", None):
                continue

            catalog_entry = catalog.get_stream(stream_id)
            if catalog_entry is None:
                LOGGER.info("Stream %s not found in catalog — skipping", stream_id)
                continue

            stream_obj = stream_class(client, catalog_entry)
            write_schema(stream_obj, client, set(STREAMS.keys()), catalog)

            if not stream_obj.is_selected() and not stream_obj.child_to_sync:
                LOGGER.info("Stream %s not selected — skipping", stream_id)
                continue

            update_currently_syncing(state, stream_id)
            LOGGER.info("Syncing stream: %s", stream_id)
            count = stream_obj.sync(state=state, transformer=transformer)
            LOGGER.info("Synced %d records for stream: %s", count, stream_id)
            singer.write_state(state)

    update_currently_syncing(state, None)
