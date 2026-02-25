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
    currently_syncing = state.get("currently_syncing")
    if not currently_syncing or currently_syncing not in selected:
        return selected
    bookmarks = state.get("bookmarks", {})
    not_yet_started = [s for s in selected if s != currently_syncing and s not in bookmarks]
    already_completed = [s for s in selected if s != currently_syncing and s in bookmarks]
    return [currently_syncing] + not_yet_started + already_completed


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
            write_schema(stream_obj, client, set(STREAMS.keys()), catalog)
            if not stream_obj.is_selected() and not stream_obj.child_to_sync:
                LOGGER.info("Stream %s not selected - skipping", stream_id)
                continue
            update_currently_syncing(state, stream_id)
            LOGGER.info("Syncing stream: %s", stream_id)
            count = stream_obj.sync(state=state, transformer=transformer)
            LOGGER.info("Synced %d records for stream: %s", count, stream_id)
            singer.write_state(state)
    update_currently_syncing(state, None)
