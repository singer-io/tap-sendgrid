"""Senders stream — verified sender identities from /v3/senders."""
from typing import Any, Dict, Optional, Tuple

from tap_sendgrid.streams.abstracts import FullTableStream


class Senders(FullTableStream):
    """Full-table stream for SendGrid verified sender identities."""

    tap_stream_id = "senders"
    replication_method = "FULL_TABLE"
    replication_keys: Tuple[str, ...] = tuple()
    key_properties: Tuple[str, ...] = ("id",)
    path = "/v3/senders"
    data_key = None  # Response is a plain list

    def next_page_params(self, response: Any) -> Optional[Dict]:
        """Senders returns a plain list — no pagination metadata."""
        return None
