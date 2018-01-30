#!/usr/bin/env python3

import singer
from singer.catalog import Catalog, CatalogEntry, Schema
from singer.utils import parse_args
from singer import metadata

from . import streams
from .context import Context
from .http import authed_get
from .streams import Scopes
from .syncs import Syncer

LOGGER = singer.get_logger()


REQUIRED_CONFIG_KEYS = ["start_date", 'api_key']


def check_credentials_are_authorized(ctx):
    res = authed_get(Scopes.source, Scopes.endpoint, ctx.config)
    scopes = res.json().get('scopes', [])

    missing_auths = set(Scopes.scopes)
    for s in Scopes.scopes:
        if s in scopes:
            missing_auths.remove(s)

    if len(missing_auths):
        raise Exception('Insufficient authorization, missing for {}'.format(
            ','.join(missing_auths)
        ))


def discover(ctx):
    check_credentials_are_authorized(ctx)
    catalog = Catalog([])
    for stream in streams.STREAMS:
        schema = Schema.from_dict(streams.load_schema(stream.tap_stream_id),
                                  inclusion="available")

        mdata = metadata.new()

        for prop in schema.properties:
            if prop in streams.PK_FIELDS[stream.tap_stream_id]:
                mdata = metadata.write(mdata, ('properties', prop), 'inclusion', 'automatic')
            else:
                mdata = metadata.write(mdata, ('properties', prop), 'inclusion', 'available')

        catalog.streams.append(CatalogEntry(
            stream=stream.tap_stream_id,
            tap_stream_id=stream.tap_stream_id,
            key_properties=streams.PK_FIELDS[stream.tap_stream_id],
            schema=schema,
            metadata=metadata.to_list(mdata)
        ))
    return catalog


def desired_fields(selected, stream_schema):
    '''
    Returns fields that should be synced
    '''
    all_fields = set()
    available = set()
    automatic = set()

    for field, field_schema in stream_schema.properties.items():
        all_fields.add(field)
        inclusion = field_schema.inclusion
        if inclusion == 'automatic':
            automatic.add(field)
        elif inclusion == 'available':
            available.add(field)
        else:
            raise Exception('Unknown inclusion ' + inclusion)

    not_selected_but_automatic = automatic.difference(selected)
    if not_selected_but_automatic:
        LOGGER.warning(
            'Fields %s are required but were not selected. Adding them.',
            not_selected_but_automatic)

    return selected.intersection(available).union(automatic)


def sync(ctx):
    check_credentials_are_authorized(ctx)

    for c in ctx.selected_catalog:
        selected_fields = set(
            [k for k, v in c.schema.properties.items()
             if v.selected or k == c.replication_key])
        fields = desired_fields(selected_fields, c.schema)

        schema = Schema(
            type='object',
            properties={prop: c.schema.properties[prop] for prop in fields}
        )
        c.schema = schema
        streams.write_schema(c.tap_stream_id, schema)

    syncer = Syncer(ctx)
    syncer.sync()


def main_impl():
    args = parse_args(REQUIRED_CONFIG_KEYS)
    ctx = Context(args.config, args.state)
    if args.discover:
        discover(ctx).dump()
    else:
        ctx.catalog = Catalog.from_dict(args.properties) \
            if args.properties else discover(ctx)
        sync(ctx)


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise

if __name__ == '__main__':
    main()