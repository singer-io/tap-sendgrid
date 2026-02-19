"""Stream definitions for SendGrid tap."""
from collections import namedtuple

from singer import write_schema

from tap_sendgrid.streams.global_suppressions import GlobalSuppressions
from tap_sendgrid.streams.groups_all import GroupsAll
from tap_sendgrid.streams.groups_members import GroupsMembers
from tap_sendgrid.streams.lists_all import ListsAll
from tap_sendgrid.streams.segments_all import SegmentsAll
from tap_sendgrid.streams.templates_all import TemplatesAll
from tap_sendgrid.streams.invalids import Invalids
from tap_sendgrid.streams.bounces import Bounces
from tap_sendgrid.streams.blocks import Blocks
from tap_sendgrid.streams.spam_reports import SpamReports
from tap_sendgrid.streams.campaigns import Campaigns
from tap_sendgrid.streams.marketing_contacts_count import MarketingContactsCount
from tap_sendgrid.streams.marketing_field_definitions import MarketingFieldDefinitions
from tap_sendgrid.streams.marketing_stats_singlesends import MarketingStatsSinglesends
from tap_sendgrid.streams.senders import Senders

# ---------------------------------------------------------------------------
# STREAM_CLASSES: dict mapping stream_id → stream class.
# Used by schema.py and sync.py.
# ---------------------------------------------------------------------------
STREAM_CLASSES = {
    "global_suppressions": GlobalSuppressions,
    "groups_all": GroupsAll,
    "groups_members": GroupsMembers,
    "lists_all": ListsAll,
    "segments_all": SegmentsAll,
    "templates_all": TemplatesAll,
    "invalids": Invalids,
    "bounces": Bounces,
    "blocks": Blocks,
    "spam_reports": SpamReports,
    "campaigns": Campaigns,
    "marketing_contacts_count": MarketingContactsCount,
    "marketing_field_definitions": MarketingFieldDefinitions,
    "marketing_stats_singlesends": MarketingStatsSinglesends,
    "senders": Senders,
}

# ---------------------------------------------------------------------------
# Legacy symbols – kept for backward compatibility with syncs.py, utils.py,
# context.py, and the unit test suite.
# ---------------------------------------------------------------------------

class Scopes(object):
    source = 'auth_check'
    endpoint = 'https://api.sendgrid.com/v3/scopes'
    scopes = [
        'suppression.read',
        'asm.groups.read',
        'templates.read',
        'templates.versions.read',
        'marketing.read'
    ]


class IDS(object):
    GLOBAL_SUPPRESSIONS = "global_suppressions"
    GROUPS_ALL = "groups_all"
    GROUPS_MEMBERS = "groups_members"
    LISTS_ALL = "lists_all"
    SEGMENTS_ALL = "segments_all"
    TEMPLATES_ALL = "templates_all"
    INVALIDS = "invalids"
    BOUNCES = "bounces"
    BLOCKS = "blocks"
    SPAM_REPORTS = "spam_reports"
    CAMPAIGNS = "campaigns"
    MARKETING_CONTACTS_COUNT = "marketing_contacts_count"
    MARKETING_FIELD_DEFINITIONS = "marketing_field_definitions"
    MARKETING_STATS_SINGLESENDS = "marketing_stats_singlesends"
    SENDERS = "senders"


PK_FIELDS = {
    IDS.GLOBAL_SUPPRESSIONS: ["email"],
    IDS.GROUPS_ALL: ["id"],
    IDS.GROUPS_MEMBERS: ["email", "group_id"],
    IDS.LISTS_ALL: ["id"],
    IDS.SEGMENTS_ALL: ["id"],
    IDS.TEMPLATES_ALL: ["id"],
    IDS.INVALIDS: ["email"],
    IDS.BOUNCES: ["email"],
    IDS.BLOCKS: ["email"],
    IDS.SPAM_REPORTS: ["email"],
    IDS.CAMPAIGNS: ["id"],
    IDS.MARKETING_CONTACTS_COUNT: [],
    IDS.MARKETING_FIELD_DEFINITIONS: ["id"],
    IDS.MARKETING_STATS_SINGLESENDS: ["id"],
    IDS.SENDERS: ["id"],
}


class BOOKMARKS(object):
    GLOBAL_SUPPRESSIONS = [IDS.GLOBAL_SUPPRESSIONS, "end_time"]
    GROUPS_MEMBERS = [IDS.GROUPS_MEMBERS, "member_count"]
    INVALIDS = [IDS.INVALIDS, "end_time"]
    BOUNCES = [IDS.BOUNCES, "end_time"]
    BLOCKS = [IDS.BLOCKS, "end_time"]
    SPAM_REPORTS = [IDS.SPAM_REPORTS, "end_time"]


_Stream = namedtuple("Stream", ("tap_stream_id", "bookmark", "endpoint", "parent"))

# Legacy STREAMS namedtuple list – iterated by syncs.py, utils.py, and tests.
STREAMS = [
    _Stream(IDS.GLOBAL_SUPPRESSIONS, BOOKMARKS.GLOBAL_SUPPRESSIONS,
            'https://api.sendgrid.com/v3/suppression/unsubscribes', parent=None),
    _Stream(IDS.GROUPS_ALL, None,
            'https://api.sendgrid.com/v3/asm/groups', parent=None),
    _Stream(IDS.GROUPS_MEMBERS, BOOKMARKS.GROUPS_MEMBERS,
            'https://api.sendgrid.com/v3/asm/groups/{}/suppressions',
            parent=IDS.GROUPS_ALL),
    _Stream(IDS.LISTS_ALL, None,
            'https://api.sendgrid.com/v3/marketing/lists', parent=None),
    _Stream(IDS.SEGMENTS_ALL, None,
            'https://api.sendgrid.com/v3/marketing/segments/2.0', parent=None),
    _Stream(IDS.TEMPLATES_ALL, None,
            'https://api.sendgrid.com/v3/templates', parent=None),
    _Stream(IDS.INVALIDS, BOOKMARKS.INVALIDS,
            'https://api.sendgrid.com/v3/suppression/invalid_emails', parent=None),
    _Stream(IDS.BOUNCES, BOOKMARKS.BOUNCES,
            'https://api.sendgrid.com/v3/suppression/bounces', parent=None),
    _Stream(IDS.BLOCKS, BOOKMARKS.BLOCKS,
            'https://api.sendgrid.com/v3/suppression/blocks', parent=None),
    _Stream(IDS.SPAM_REPORTS, BOOKMARKS.SPAM_REPORTS,
            'https://api.sendgrid.com/v3/suppression/spam_reports', parent=None),
    _Stream(IDS.CAMPAIGNS, None,
            'https://api.sendgrid.com/v3/marketing/singlesends', parent=None),
    _Stream(IDS.MARKETING_CONTACTS_COUNT, None,
            'https://api.sendgrid.com/v3/marketing/contacts/count', parent=None),
    _Stream(IDS.MARKETING_FIELD_DEFINITIONS, None,
            'https://api.sendgrid.com/v3/marketing/field_definitions', parent=None),
    _Stream(IDS.MARKETING_STATS_SINGLESENDS, None,
            'https://api.sendgrid.com/v3/marketing/stats/singlesends', parent=None),
    _Stream(IDS.SENDERS, None,
            'https://api.sendgrid.com/v3/senders', parent=None),
]

