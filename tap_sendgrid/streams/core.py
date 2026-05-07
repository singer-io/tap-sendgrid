"""Backward-compatibility shim.

All stream classes live in their own modules.  Import from them directly or
use ``tap_sendgrid.streams.STREAMS`` for the full registry.
"""

from tap_sendgrid.streams.contacts import (  # noqa: F401
    MarketingContactsCount,
    MarketingFieldDefinitions,
)
from tap_sendgrid.streams.marketing import (  # noqa: F401
    Lists,
    Segments,
    SingleSendStats,
    SingleSends,
    StatsAutomations,
)
from tap_sendgrid.streams.senders import Senders  # noqa: F401
from tap_sendgrid.streams.suppression import (  # noqa: F401
    Blocks,
    Bounces,
    GlobalSuppressions,
    InvalidEmails,
    SpamReports,
)
from tap_sendgrid.streams.suppression_groups import (  # noqa: F401
    SuppressionGroupMembers,
    SuppressionGroups,
)
from tap_sendgrid.streams.templates import Templates  # noqa: F401

__all__ = [
    "Blocks",
    "Bounces",
    "GlobalSuppressions",
    "InvalidEmails",
    "Lists",
    "MarketingContactsCount",
    "MarketingFieldDefinitions",
    "Segments",
    "Senders",
    "SingleSends",
    "SingleSendStats",
    "SpamReports",
    "StatsAutomations",
    "SuppressionGroupMembers",
    "SuppressionGroups",
    "Templates",
]
