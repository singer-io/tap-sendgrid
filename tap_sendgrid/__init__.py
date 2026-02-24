"""tap-sendgrid — Singer tap entry point.

Exposes ``do_discover`` and ``main`` which are invoked by the console script
defined in setup.py.
"""
import json
import sys

import singer

from tap_sendgrid.client import Client
from tap_sendgrid.discover import discover
from tap_sendgrid.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["start_date"]


def do_discover() -> None:
    """Run discovery mode and write the catalog to stdout as JSON."""
    LOGGER.info("Starting discover")
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    LOGGER.info("Finished discover")


@singer.utils.handle_top_exception(LOGGER)
def main() -> None:
    """Parse CLI arguments and dispatch to discover or sync mode."""
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    state = parsed_args.state or {}

    with Client(parsed_args.config) as client:
        if parsed_args.discover:
            do_discover()
        elif parsed_args.catalog:
            sync(
                client=client,
                catalog=parsed_args.catalog,
                state=state,
            )


if __name__ == "__main__":
    main()
