"""Full-table suppression group streams.

Covers: ``suppression_groups`` (parent) and ``suppression_group_members`` (child).
"""
from typing import Any, Dict, List, Optional, Tuple

from tap_sendgrid.streams.abstracts import FullTableStream


class SuppressionGroups(FullTableStream):
    """Full-table stream for SendGrid ASM suppression groups."""

    tap_stream_id = "suppression_groups"
    replication_method = "FULL_TABLE"
    replication_keys: Tuple[str, ...] = tuple()
    key_properties: Tuple[str, ...] = ("id",)
    path = "/v3/asm/groups"
    data_key = None
    children = ["suppression_group_members"]

    def next_page_params(self, response: Any) -> Optional[Dict]:
        """Return None — the ASM groups endpoint returns a plain list with no pagination."""
        # ASM groups endpoint returns a plain list — no pagination
        return None


class SuppressionGroupMembers(FullTableStream):
    """Full-table child stream for members of each SendGrid ASM suppression group."""

    tap_stream_id = "suppression_group_members"
    replication_method = "FULL_TABLE"
    replication_keys: Tuple[str, ...] = tuple()
    key_properties: Tuple[str, ...] = ("group_id", "recipient_email")
    path = "/v3/asm/groups/{}/suppressions"
    data_key = None
    parent = "suppression_groups"

    def get_path(self, parent_obj: Optional[Dict] = None) -> str:
        """Format the path with the parent group ID."""
        if parent_obj:
            return self.path.format(parent_obj["id"])
        return self.path

    def parse_records(self, response: Any) -> List[Dict]:
        """Flatten the suppression list response into dicts with a ``group_id`` field."""
        records = []
        for item in response if isinstance(response, list) else []:
            records.append(
                {
                    "group_id": item.get("group_id"),
                    "recipient_email": item.get("recipient_email") or item.get("email"),
                    "created": item.get("created"),
                }
            )
        return records

    def next_page_params(self, response: Any) -> Optional[Dict]:
        # Suppression members endpoint returns a plain list — no pagination
        return None
