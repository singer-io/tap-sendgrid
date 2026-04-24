"""Contacts-related streams: MarketingContactsCount and MarketingFieldDefinitions."""
from typing import Any, Dict, List, Tuple

from tap_sendgrid.streams.abstracts import FullTableStream


class MarketingContactsCount(FullTableStream):
    """Full-table stream for the SendGrid marketing contacts aggregate count.

    The endpoint ``GET /v3/marketing/contacts/count`` returns a single
    summary object with no natural unique identifier.  ``key_properties``
    is intentionally empty — this is a valid Singer append-only stream.
    """

    tap_stream_id = "marketing_contacts_count"
    key_properties: Tuple[str, ...] = tuple()
    path = "/v3/marketing/contacts/count"
    data_key = None

    def parse_records(self, response: Any) -> List[Dict]:
        """Yield the entire response dict as a single record."""
        if isinstance(response, dict) and response:
            return [response]
        return []


class MarketingFieldDefinitions(FullTableStream):
    """Full-table stream for SendGrid marketing field definitions.

    The endpoint returns ``{reserved_fields: [...], custom_fields: [...]}``.
    Both arrays are merged and emitted as individual records.
    """

    tap_stream_id = "marketing_field_definitions"
    key_properties: Tuple[str, ...] = ("id",)
    path = "/v3/marketing/field_definitions"
    data_key = None

    def parse_records(self, response: Any) -> List[Dict]:
        """Merge reserved_fields and custom_fields into a single record list."""
        if not isinstance(response, dict):
            return []
        reserved = response.get("reserved_fields", []) or []
        custom = response.get("custom_fields", []) or []
        return list(reserved) + list(custom)
