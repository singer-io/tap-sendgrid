"""MarketingContactsCount stream for SendGrid tap."""
from tap_sendgrid.streams.abstracts import FullTableStream, BASE_URL
from tap_sendgrid.http import authed_get


class MarketingContactsCount(FullTableStream):
    """Stream for retrieving marketing contacts count."""
    tap_stream_id = "marketing_contacts_count"
    key_properties = []  # This stream returns a single aggregate record
    replication_keys = []
    path = "marketing/contacts/count"
    data_key = "result"

    def get_records(self, config):
        """Override to handle single object response."""
        endpoint = f"{BASE_URL}/{self.path}"
        response = authed_get(
            self.tap_stream_id,
            endpoint,
            config,
            params=self.params
        )

        data = response.json()
        # This endpoint returns a single object, not an array
        if data:
            yield data
