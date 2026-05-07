"""Full-table marketing streams.

Covers: lists, segments, single_sends, single_send_stats, stats_automations.
"""
from typing import Tuple

from tap_sendgrid.streams.abstracts import FullTableStream


class Lists(FullTableStream):
    """Full-table stream for SendGrid marketing contact lists."""

    tap_stream_id = "lists"
    key_properties: Tuple[str, ...] = ("id",)
    path = "/v3/marketing/lists"
    data_key = "result"


class Segments(FullTableStream):
    """Full-table stream for SendGrid marketing audience segments."""

    tap_stream_id = "segments"
    key_properties: Tuple[str, ...] = ("id",)
    path = "/v3/marketing/segments"
    data_key = "results"


class SingleSends(FullTableStream):
    """Full-table stream for SendGrid single-send (one-time) emails."""

    tap_stream_id = "single_sends"
    key_properties: Tuple[str, ...] = ("id",)
    path = "/v3/marketing/singlesends"
    data_key = "result"


class SingleSendStats(FullTableStream):
    """Full-table stream for per-single-send delivery and engagement statistics."""

    tap_stream_id = "single_send_stats"
    key_properties: Tuple[str, ...] = ("id",)
    path = "/v3/marketing/stats/singlesends"
    data_key = "results"


class StatsAutomations(FullTableStream):
    """Full-table stream for SendGrid automation email statistics."""

    tap_stream_id = "stats_automations"
    key_properties: Tuple[str, ...] = ("id",)
    path = "/v3/marketing/stats/automations"
    data_key = "results"
