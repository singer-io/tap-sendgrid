"""Schema loading utilities for SendGrid tap."""
import os
import json
from typing import Dict, Tuple

import singer
from singer import metadata
from tap_sendgrid.streams import STREAM_CLASSES as STREAMS

LOGGER = singer.get_logger()


def get_abs_path(path: str) -> str:
    """
    Get the absolute path for the schema files.
    """
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def get_schemas() -> Tuple[Dict, Dict]:
    """
    Load schemas from schema files and prepare metadata for each stream.
    Returns schema and metadata for the catalog.
    """
    schemas = {}
    field_metadata = {}

    for stream_name, stream_class in STREAMS.items():
        schema_path = get_abs_path(f"schemas/{stream_name}.json")
        with open(schema_path, encoding='utf-8') as file:
            schema = json.load(file)

        schemas[stream_name] = schema

        # Create metadata
        mdata = metadata.new()
        
        # Get stream properties
        # Instantiate stream to get properties
        stream_obj = stream_class(client=None, catalog=None, config={})
        
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream_obj.key_properties,
            valid_replication_keys=stream_obj.replication_keys,
            replication_method=stream_obj.replication_method,
        )
        mdata = metadata.to_map(mdata)

        # Mark stream as selected by default for discovery
        mdata = metadata.write(mdata, (), "selected", True)

        # Mark primary keys as automatic
        for key_property in stream_obj.key_properties:
            mdata = metadata.write(
                mdata, ("properties", key_property), "inclusion", "automatic"
            )

        # Mark replication keys as automatic
        for replication_key in stream_obj.replication_keys:
            mdata = metadata.write(
                mdata, ("properties", replication_key), "inclusion", "automatic"
            )

        # Add parent stream metadata if applicable
        if stream_obj.parent:
            mdata = metadata.write(mdata, (), 'parent-tap-stream-id', stream_obj.parent)

        mdata = metadata.to_list(mdata)
        field_metadata[stream_name] = mdata

    return schemas, field_metadata
