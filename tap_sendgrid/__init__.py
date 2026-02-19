#!/usr/bin/env python3

import singer
from singer.utils import parse_args
from tap_sendgrid.discover import discover
from tap_sendgrid.sync import sync, check_credentials_are_authorized
from tap_sendgrid.http import authed_get

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["start_date", 'api_key']


def main_impl():
    args = parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config
    state = args.state

    if args.discover:
        catalog = discover()
        catalog.dump()
    elif args.catalog:
        sync(config, args.catalog, state)
    else:
        LOGGER.info("No Catalog was provided")
        catalog = discover()
        catalog.dump()


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise


if __name__ == '__main__':
    main()
