"""Incremental suppression streams.

Covers: blocks, bounces, spam_reports, invalid_emails, global_suppressions.
"""
from typing import Tuple

from tap_sendgrid.streams.abstracts import IncrementalStream


class _SuppressionIncrementalStream(IncrementalStream):
    """Shared base for unix-timestamp incremental suppression streams."""

    replication_keys: Tuple[str, ...] = ("created",)
    key_properties: Tuple[str, ...] = ("email",)
    data_key = None
    cursor_type = "unix"


class Blocks(_SuppressionIncrementalStream):
    """Incremental stream for SendGrid email blocks."""

    tap_stream_id = "blocks"
    path = "/v3/suppression/blocks"


class Bounces(_SuppressionIncrementalStream):
    """Incremental stream for SendGrid email bounces."""

    tap_stream_id = "bounces"
    path = "/v3/suppression/bounces"


class SpamReports(_SuppressionIncrementalStream):
    """Incremental stream for SendGrid spam reports."""

    tap_stream_id = "spam_reports"
    path = "/v3/suppression/spam_reports"


class InvalidEmails(_SuppressionIncrementalStream):
    """Incremental stream for SendGrid invalid email addresses."""

    tap_stream_id = "invalid_emails"
    path = "/v3/suppression/invalid_emails"


class GlobalSuppressions(_SuppressionIncrementalStream):
    """Incremental stream for SendGrid global unsubscribes."""

    tap_stream_id = "global_suppressions"
    path = "/v3/suppression/unsubscribes"
