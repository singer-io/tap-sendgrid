"""Discovery mode helpers for tap-sendgrid.

Builds the Singer Catalog from JSON schema files and stream metadata.
"""
from singer import get_logger
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema

from tap_sendgrid.schema import get_schemas

LOGGER = get_logger()


def discover() -> Catalog:
    """Build and return a Singer Catalog from all registered streams."""
    schemas, field_metadata = get_schemas()
    catalog = Catalog(streams=[])

    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict)
        mdata = field_metadata[stream_name]
        key_properties = metadata.to_map(mdata).get((), {}).get("table-key-properties")

        catalog.streams.append(
            CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=key_properties,
                schema=schema,
                metadata=mdata,
            )
        )

    return catalog
