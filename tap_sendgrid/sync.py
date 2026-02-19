"""Sync mode for SendGrid tap."""
from typing import Dict

import singer
from tap_sendgrid.streams import STREAM_CLASSES as STREAMS
from tap_sendgrid.streams.abstracts import BASE_URL
from tap_sendgrid import http

LOGGER = singer.get_logger()


def check_credentials_are_authorized(config):
    """Verify API key has required scopes."""
    res = http.authed_get("scopes", f"{BASE_URL}/scopes", config)
    scopes = res.json().get('scopes', [])

    required_scopes = ['suppression.read', 'templates.read', 'marketing.read']
    missing_auths = []
    
    for scope in required_scopes:
        if scope not in scopes:
            missing_auths.append(scope)

    if missing_auths:
        raise Exception('Insufficient authorization, missing for {}'.format(
            ','.join(missing_auths)
        ))


def update_currently_syncing(state: Dict, stream_name: str = None) -> None:
    """Update currently_syncing in state and write it."""
    if not stream_name and singer.get_currently_syncing(state):
        del state["currently_syncing"]
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def write_schema_for_stream(stream, catalog, config) -> None:
    """Write schema for stream and its children."""
    if stream.is_selected():
        stream.write_schema()

    # Handle children
    for child_name in stream.children:
        child_catalog_entry = catalog.get_stream(child_name)
        if child_catalog_entry:
            child_obj = STREAMS[child_name](client=None, catalog=child_catalog_entry, config=config)
            write_schema_for_stream(child_obj, catalog, config)
            if child_name in [s.stream for s in catalog.get_selected_streams({})]:
                stream.child_to_sync.append(child_obj)


def sync(config: Dict, catalog: singer.Catalog, state: Dict) -> None:
    """
    Sync selected streams from catalog.
    """
    # Check credentials first
    check_credentials_are_authorized(config)

    streams_to_sync = []
    for stream in catalog.get_selected_streams(state):
        streams_to_sync.append(stream.stream)
    LOGGER.info("selected_streams: %s", streams_to_sync)

    last_stream = singer.get_currently_syncing(state)
    LOGGER.info("last/currently syncing stream: %s", last_stream)

    # Build complete list of streams including required parents before syncing
    streams_with_parents = []
    for stream_name in streams_to_sync:
        stream_catalog_entry = catalog.get_stream(stream_name)
        stream = STREAMS[stream_name](client=None, catalog=stream_catalog_entry, config=config)
        
        if stream.parent and stream.parent not in streams_with_parents:
            streams_with_parents.append(stream.parent)
        if stream_name not in streams_with_parents:
            streams_with_parents.append(stream_name)

    with singer.Transformer() as transformer:
        for stream_name in streams_with_parents:
            stream_catalog_entry = catalog.get_stream(stream_name)
            if not stream_catalog_entry:
                LOGGER.warning(f"Skipping {stream_name} - not in catalog")
                continue

            stream = STREAMS[stream_name](client=None, catalog=stream_catalog_entry, config=config)
            
            # Skip child streams - they are handled by their parents
            if stream.parent:
                continue

            write_schema_for_stream(stream, catalog, config)
            LOGGER.info("START Syncing: %s", stream_name)
            update_currently_syncing(state, stream_name)

            try:
                total_records = stream.sync(state=state, transformer=transformer)
            except Exception as error:
                # Log error but continue with other streams for authorization issues
                error_msg = str(error)
                if "401" in error_msg or "Unauthorized" in error_msg:
                    LOGGER.warning(
                        "Skipping stream %s: %s (authorization required)",
                        stream_name,
                        error_msg
                    )
                    update_currently_syncing(state, None)
                    continue
                else:
                    LOGGER.error(
                        "Error syncing stream %s: %s",
                        stream_name,
                        error_msg
                    )
                    raise

            update_currently_syncing(state, None)
            LOGGER.info(
                "FINISHED Syncing: %s, total_records: %s",
                stream_name, total_records
            )
