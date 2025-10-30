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
        'marketing_campaigns.read',
        'templates.read',
        'templates.versions.read'
    ]


class IDS(object):
    GLOBAL_SUPPRESSIONS = "global_suppressions"
    GROUPS_ALL = "groups_all"
    GROUPS_MEMBERS = "groups_members"
    CONTACTS = "contacts"
    LISTS_ALL = "lists_all"
    LISTS_MEMBERS = "lists_members"
    SEGMENTS_ALL = "segments_all"
    SEGMENTS_MEMBERS = "segments_members"
    TEMPLATES_ALL = "templates_all"
    INVALIDS = "invalids"
    BOUNCES = "bounces"
    BLOCKS = "blocks"
    SPAM_REPORTS = "spam_reports"
    CAMPAIGNS = "campaigns"


stream_ids = [getattr(IDS, x) for x in dir(IDS)]

PK_FIELDS = {
    IDS.GLOBAL_SUPPRESSIONS: ["email"],
    IDS.GROUPS_ALL: ["id"],
    IDS.GROUPS_MEMBERS: ["email"],
    IDS.CONTACTS: ["id"],
    IDS.LISTS_ALL: ["id"],
    IDS.LISTS_MEMBERS: ["id"],
    IDS.SEGMENTS_ALL: ["id"],
    IDS.SEGMENTS_MEMBERS: ["id"],
    IDS.TEMPLATES_ALL: ["id"],
    IDS.INVALIDS: ["email"],
    IDS.BOUNCES: ["email"],
    IDS.BLOCKS: ["email"],
    IDS.SPAM_REPORTS: ["email"],
    IDS.CAMPAIGNS: ["id"],
}


class BOOKMARKS(object):
    GLOBAL_SUPPRESSIONS = [IDS.GLOBAL_SUPPRESSIONS, "end_time"]
    GROUPS_MEMBERS = [IDS.GROUPS_MEMBERS, "member_count"]
    CONTACTS = [IDS.CONTACTS, "timestamp"]
    LISTS_MEMBERS = [IDS.LISTS_MEMBERS, "member_count"]
    SEGMENTS_MEMBERS = [IDS.SEGMENTS_MEMBERS, "member_count"]
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
        IDS.CONTACTS,
        BOOKMARKS.CONTACTS,
        'https://api.sendgrid.com/v3/contactdb/recipients/search',
        parent=None
    ),
    Stream(
        IDS.LISTS_ALL,
        None,
        'https://api.sendgrid.com/v3/contactdb/lists',
        parent=None
    ),
    Stream(
        IDS.LISTS_MEMBERS,
        BOOKMARKS.LISTS_MEMBERS,
        'https://api.sendgrid.com/v3/contactdb/lists/{}/recipients',
        parent=IDS.LISTS_ALL
    ),
    Stream(
        IDS.SEGMENTS_ALL,
        None,
        'https://api.sendgrid.com/v3/contactdb/segments',
        parent=None
    ),
    Stream(
        IDS.SEGMENTS_MEMBERS,
        BOOKMARKS.SEGMENTS_MEMBERS,
        'https://api.sendgrid.com/v3/contactdb/segments/{}/recipients',
        parent=IDS.SEGMENTS_ALL
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
        'https://api.sendgrid.com/v3/campaigns',
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