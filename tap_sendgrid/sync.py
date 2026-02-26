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
    """Update the currently_syncing key in state and write it."""
    if stream_name:
        state["currently_syncing"] = stream_name
    else:
        state.pop("currently_syncing", None)
    singer.write_state(state)


def _order_streams_for_resume(selected, state):
    """Return selected streams in resume order, starting from currently_syncing.

    Uses circular ordering based on the STREAMS dict position: start at the
    interrupted stream and continue through the remainder of the list, wrapping
    around to streams that appeared before it.  This replicates the original
    sync sequence as closely as possible regardless of what bookmarks exist in
    the incoming state (which may be a merge of a previous run and a manipulated
    state in integration tests).
    """
    currently_syncing = state.get("currently_syncing")
    if not currently_syncing or currently_syncing not in selected:
        return selected
    idx = selected.index(currently_syncing)
    return selected[idx:] + selected[:idx]


def sync(client, catalog, state):
    """Orchestrate sync for all selected streams."""
    with Transformer() as transformer:
        selected = [
            stream_id
            for stream_id, stream_class in STREAMS.items()
            if not getattr(stream_class, "parent", None)
            and catalog.get_stream(stream_id) is not None
        ]
        ordered = _order_streams_for_resume(selected, state)
        for stream_id in ordered:
            stream_class = STREAMS[stream_id]
            catalog_entry = catalog.get_stream(stream_id)
            stream_obj = stream_class(client, catalog_entry)
            write_schema(stream_obj, client, catalog)
            if not stream_obj.is_selected() and not stream_obj.child_to_sync:
                LOGGER.info("Stream %s not selected - skipping", stream_id)
                continue
            update_currently_syncing(state, stream_id)
            LOGGER.info("Syncing stream: %s", stream_id)
            count = stream_obj.sync(state=state, transformer=transformer)
            LOGGER.info("Synced %d records for stream: %s", count, stream_id)
            singer.write_state(state)
    update_currently_syncing(state, None)
