"""Senders stream — verified sender identities from /v3/senders."""
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional, Tuple

from tap_sendgrid.streams.abstracts import FullTableStream


class Senders(FullTableStream):
    """Full-table stream for SendGrid verified sender identities."""

    tap_stream_id = "senders"
    key_properties: Tuple[str, ...] = ("id",)
    path = "/v3/senders"
    data_key = None  # Response is a plain list

    def get_records(
        self,
        params: Optional[Dict[str, Any]] = None,
        parent_obj: Optional[Dict[str, Any]] = None,
    ) -> Iterable[Dict[str, Any]]:
        """Yield records with unix integer timestamps normalised to ISO-8601 strings.

        The SendGrid senders API returns ``created_at`` and ``updated_at`` as
        Unix epoch integers.  We convert them to ISO-8601 strings here so that
        the tap emits consistent RFC 3339 datetime values across all streams,
        and the schema can declare these fields as ``"format": "date-time"``.
        """
        for record in super().get_records(params=params, parent_obj=parent_obj):
            for field in ("created_at", "updated_at"):
                value = record.get(field)
                if isinstance(value, int):
                    record[field] = datetime.fromtimestamp(value, tz=timezone.utc).isoformat()
            yield record
