import os
import json
from collections import namedtuple
import singer


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


stream_ids = [getattr(IDS, x) for x in dir(IDS)]

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
    IDS.MARKETING_CONTACTS_COUNT: [],  # Singleton resource, no primary key
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


Stream = namedtuple("Stream", ("tap_stream_id", "bookmark", "endpoint", "parent"))
STREAMS = [
    Stream(
        IDS.GLOBAL_SUPPRESSIONS,
        BOOKMARKS.GLOBAL_SUPPRESSIONS,
        'https://api.sendgrid.com/v3/suppression/unsubscribes',
        parent=None
    ),
    Stream(
        IDS.GROUPS_ALL,
        None,
        'https://api.sendgrid.com/v3/asm/groups',
        parent=None
    ),
    Stream(
        IDS.GROUPS_MEMBERS,
        BOOKMARKS.GROUPS_MEMBERS,
        'https://api.sendgrid.com/v3/asm/groups/{}/suppressions',
        parent=IDS.GROUPS_ALL
    ),
    Stream(
        IDS.LISTS_ALL,
        None,
        'https://api.sendgrid.com/v3/marketing/lists',
        parent=None
    ),
    Stream(
        IDS.SEGMENTS_ALL,
        None,
        'https://api.sendgrid.com/v3/marketing/segments/2.0',
        parent=None
    ),
    Stream(
        IDS.TEMPLATES_ALL,
        None,
        'https://api.sendgrid.com/v3/templates',
        parent=None
    ),
    Stream(
        IDS.INVALIDS,
        BOOKMARKS.INVALIDS,
        'https://api.sendgrid.com/v3/suppression/invalid_emails',
        parent=None
    ),
    Stream(
        IDS.BOUNCES,
        BOOKMARKS.BOUNCES,
        'https://api.sendgrid.com/v3/suppression/bounces',
        parent=None
    ),
    Stream(
        IDS.BLOCKS,
        BOOKMARKS.BLOCKS,
        'https://api.sendgrid.com/v3/suppression/blocks',
        parent=None
    ),
    Stream(
        IDS.SPAM_REPORTS,
        BOOKMARKS.SPAM_REPORTS,
        'https://api.sendgrid.com/v3/suppression/spam_reports',
        parent=None
    ),
    Stream(
        IDS.CAMPAIGNS,
        None,
        'https://api.sendgrid.com/v3/marketing/singlesends',
        parent=None
    ),
    Stream(
        IDS.MARKETING_CONTACTS_COUNT,
        None,
        'https://api.sendgrid.com/v3/marketing/contacts/count',
        parent=None
    ),
    Stream(
        IDS.MARKETING_FIELD_DEFINITIONS,
        None,
        'https://api.sendgrid.com/v3/marketing/field_definitions',
        parent=None
    ),
    Stream(
        IDS.MARKETING_STATS_SINGLESENDS,
        None,
        'https://api.sendgrid.com/v3/marketing/stats/singlesends',
        parent=None
    ),
    Stream(
        IDS.SENDERS,
        None,
        'https://api.sendgrid.com/v3/senders',
        parent=None
    ),
]


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(stream_id):
    path = 'schemas/{}.json'
    return json.load(open(get_abs_path(path.format(stream_id))))


def load_and_write_schema(tap_stream_id):
    schema = load_schema(tap_stream_id)
    singer.write_schema(tap_stream_id, schema, PK_FIELDS[tap_stream_id])


def write_schema(tap_stream_id, schema):
    singer.write_schema(tap_stream_id, schema.to_dict(), PK_FIELDS[tap_stream_id])
