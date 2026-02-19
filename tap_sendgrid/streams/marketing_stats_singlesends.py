"""MarketingStatsSinglesends stream for SendGrid tap."""
from tap_sendgrid.streams.abstracts import FullTableStream


class MarketingStatsSinglesends(FullTableStream):
    """Stream for retrieving marketing single send statistics."""
    tap_stream_id = "marketing_stats_singlesends"
    key_properties = ["id"]
    replication_keys = []
    path = "marketing/stats/singlesends"
    data_key = "results"
