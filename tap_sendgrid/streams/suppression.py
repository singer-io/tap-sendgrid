"""Incremental suppression streams.

Covers: blocks, bounces, spam_reports, invalid_emails, global_suppressions.
"""
from typing import Tuple

from tap_sendgrid.streams.abstracts import IncrementalStream


class Blocks(IncrementalStream):
    """Incremental stream for SendGrid email blocks."""

    tap_stream_id = "blocks"
    replication_method = "INCREMENTAL"
    replication_keys: Tuple[str, ...] = ("created",)
    key_properties: Tuple[str, ...] = ("email",)
    path = "/v3/suppression/blocks"
    data_key = None
    cursor_type = "unix"


class Bounces(IncrementalStream):
    """Incremental stream for SendGrid email bounces."""

    tap_stream_id = "bounces"
    replication_method = "INCREMENTAL"
    replication_keys: Tuple[str, ...] = ("created",)
    key_properties: Tuple[str, ...] = ("email",)
    path = "/v3/suppression/bounces"
    data_key = None
    cursor_type = "unix"


class SpamReports(IncrementalStream):
    """Incremental stream for SendGrid spam reports."""

    tap_stream_id = "spam_reports"
    replication_method = "INCREMENTAL"
    replication_keys: Tuple[str, ...] = ("created",)
    key_properties: Tuple[str, ...] = ("email",)
    path = "/v3/suppression/spam_reports"
    data_key = None
    cursor_type = "unix"


class InvalidEmails(IncrementalStream):
    """Incremental stream for SendGrid invalid email addresses."""

    tap_stream_id = "invalid_emails"
    replication_method = "INCREMENTAL"
    replication_keys: Tuple[str, ...] = ("created",)
    key_properties: Tuple[str, ...] = ("email",)
    path = "/v3/suppression/invalid_emails"
    data_key = None
    cursor_type = "unix"


class GlobalSuppressions(IncrementalStream):
    """Incremental stream for SendGrid global unsubscribes."""

    tap_stream_id = "global_suppressions"
    replication_method = "INCREMENTAL"
    replication_keys: Tuple[str, ...] = ("created",)
    key_properties: Tuple[str, ...] = ("email",)
    path = "/v3/suppression/unsubscribes"
    data_key = None
    cursor_type = "unix"
