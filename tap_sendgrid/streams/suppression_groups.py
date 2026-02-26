"""Full-table suppression group streams.

Covers: ``suppression_groups`` (parent) and ``suppression_group_members`` (child).
"""
from typing import Any, Dict, Iterable, Optional, Tuple

from tap_sendgrid.streams.abstracts import FullTableStream


class SuppressionGroups(FullTableStream):
    """Full-table stream for SendGrid ASM suppression groups."""

    tap_stream_id = "suppression_groups"
    key_properties: Tuple[str, ...] = ("id",)
    path = "/v3/asm/groups"
    data_key = None
    children = ["suppression_group_members"]


class SuppressionGroupMembers(FullTableStream):
    """Full-table child stream for members of each SendGrid ASM suppression group.

    The endpoint ``GET /v3/asm/groups/{group_id}/suppressions`` returns a plain
    list of email address strings.  ``get_records`` is overridden to inject the
    parent ``group_id`` into each record dict.
    """

    tap_stream_id = "suppression_group_members"
    key_properties: Tuple[str, ...] = ("group_id", "recipient_email")
    path = "/v3/asm/groups/{}/suppressions"
    data_key = None
    parent = "suppression_groups"

    def get_records(
        self,
        params: Optional[Dict[str, Any]] = None,
        parent_obj: Optional[Dict[str, Any]] = None,
    ) -> Iterable[Dict[str, Any]]:
        """Fetch suppressions for the parent group and yield normalised record dicts.

        The API returns a JSON array of email strings; this method wraps each
        email into a ``{group_id, recipient_email}`` dict.
        """
        group_id = parent_obj["id"] if parent_obj else None
        response = self.client.get(
            self.get_path(parent_obj),
            params=params,
            stream_name=self.tap_stream_id,
        )
        for item in response if isinstance(response, list) else []:
            if isinstance(item, str):
                yield {"group_id": group_id, "recipient_email": item}
            elif isinstance(item, dict):
                yield {
                    "group_id": item.get("group_id") or group_id,
                    "recipient_email": item.get("recipient_email") or item.get("email"),
                }
