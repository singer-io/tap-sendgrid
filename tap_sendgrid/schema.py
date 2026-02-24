"""Schema loading and metadata helpers for tap-sendgrid."""
import json
import os
from typing import Dict, Tuple

from singer import metadata

from tap_sendgrid.streams import STREAMS


def get_abs_path(path: str) -> str:
    """Return the absolute path relative to this file's directory."""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def get_schemas() -> Tuple[Dict, Dict]:
    """Load JSON schemas and build Singer metadata for all registered streams."""
    schemas: Dict = {}
    field_metadata: Dict = {}

    for stream_name, stream_obj in STREAMS.items():
        schema_path = get_abs_path(f"schemas/{stream_name}.json")
        with open(schema_path, "r", encoding="utf-8") as file:
            schema = json.load(file)

        schemas[stream_name] = schema

        mdata = metadata.new()
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=getattr(stream_obj, "key_properties"),
            valid_replication_keys=(getattr(stream_obj, "replication_keys") or []),
            replication_method=getattr(stream_obj, "replication_method"),
        )
        mdata = metadata.to_map(mdata)

        automatic_keys = list(getattr(stream_obj, "replication_keys") or [])
        for field_name in schema.get("properties", {}).keys():
            if field_name in automatic_keys:
                mdata = metadata.write(mdata, ("properties", field_name), "inclusion", "automatic")

        field_metadata[stream_name] = metadata.to_list(mdata)

    return schemas, field_metadata


def write_schema(stream, client, streams_to_sync, catalog) -> None:
    """Write the Singer SCHEMA message for *stream* and configure child streams."""
    if stream.is_selected():
        stream.write_schema()

    for child in stream.children:
        child_obj = STREAMS[child](client, catalog.get_stream(child))
        write_schema(child_obj, client, streams_to_sync, catalog)
        if child in streams_to_sync:
            stream.child_to_sync.append(child_obj)
