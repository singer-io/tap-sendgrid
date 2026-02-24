"""Stream registry for tap-sendgrid.

Each stream class is imported from its own module.  The ``STREAMS`` dict maps
tap_stream_id to the corresponding class and is the single source of truth used
by the schema builder and the sync loop.
"""
from tap_sendgrid.streams.contacts import MarketingContactsCount, MarketingFieldDefinitions
from tap_sendgrid.streams.marketing import (
    Lists,
    Segments,
    SingleSendStats,
    SingleSends,
    StatsAutomations,
)
from tap_sendgrid.streams.senders import Senders
from tap_sendgrid.streams.suppression import (
    Blocks,
    Bounces,
    GlobalSuppressions,
    InvalidEmails,
    SpamReports,
)
from tap_sendgrid.streams.suppression_groups import SuppressionGroupMembers, SuppressionGroups
from tap_sendgrid.streams.templates import Templates

# Parent streams must appear before their children so that the sync loop
# drives child sync through the parent's child_to_sync list.
STREAMS = {
    "blocks": Blocks,
    "bounces": Bounces,
    "spam_reports": SpamReports,
    "invalid_emails": InvalidEmails,
    "global_suppressions": GlobalSuppressions,
    "lists": Lists,
    "segments": Segments,
    "single_sends": SingleSends,
    "single_send_stats": SingleSendStats,
    "stats_automations": StatsAutomations,
    "templates": Templates,
    "suppression_groups": SuppressionGroups,
    "suppression_group_members": SuppressionGroupMembers,
    "senders": Senders,
    "marketing_contacts_count": MarketingContactsCount,
    "marketing_field_definitions": MarketingFieldDefinitions,
}
