"""Full-table templates stream."""
from typing import Dict, Tuple

from tap_sendgrid.streams.abstracts import FullTableStream


class Templates(FullTableStream):
    """Full-table stream for SendGrid email templates (legacy and dynamic)."""

    tap_stream_id = "templates"
    replication_method = "FULL_TABLE"
    replication_keys: Tuple[str, ...] = tuple()
    key_properties: Tuple[str, ...] = ("id",)
    path = "/v3/templates"
    data_key = "result"

    def default_params(self) -> Dict:
        """Extend the default params to request both legacy and dynamic template generations."""
        params = super().default_params()
        params["generations"] = "legacy,dynamic"
        return params
