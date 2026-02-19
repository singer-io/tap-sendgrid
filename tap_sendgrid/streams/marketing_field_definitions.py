"""MarketingFieldDefinitions stream for SendGrid tap."""
from tap_sendgrid.streams.abstracts import FullTableStream, BASE_URL
from tap_sendgrid.http import authed_get


class MarketingFieldDefinitions(FullTableStream):
    """Stream for retrieving marketing field definitions."""
    tap_stream_id = "marketing_field_definitions"
    key_properties = ["id"]
    replication_keys = []
    path = "marketing/field_definitions"
    data_key = "custom_fields"

    def get_records(self, config):
        """Override to handle special response format.
        
        The field definitions endpoint returns:
        {
            "reserved_fields": [...],
            "custom_fields": [...]
        }
        
        We need to merge both arrays and yield all fields.
        """
        endpoint = f"{BASE_URL}/{self.path}"
        response = authed_get(
            self.tap_stream_id,
            endpoint,
            config,
            params=self.params
        )

        data = response.json()
        
        # Merge reserved_fields and custom_fields
        reserved_fields = data.get('reserved_fields', [])
        custom_fields = data.get('custom_fields', [])
        
        # Yield all fields
        yield from reserved_fields
        yield from custom_fields
