import singer

from .streams import PK_FIELDS, STREAMS, IDS


def get_results_from_payload(payload):
    """
    SendGrid sometimes returns Lists or one keyed Dicts.
    New Marketing API returns {'result': [...], '_metadata': {...}}
    """
    if isinstance(payload, dict):
        # New Marketing API format
        if 'result' in payload:
            return payload['result']
        # Field definitions returns both reserved and custom fields
        if 'reserved_fields' in payload:
            reserved = payload.get('reserved_fields', [])
            custom = payload.get('custom_fields', [])
            return reserved + custom
        # Singleton resources (like contacts_count) - wrap in list
        if 'contact_count' in payload:
            return [payload]
        # Legacy format - return first value
        return next(iter(payload.values()))
    else:
        return payload


def make_record_if_str(record, stream):
    """
    transform email string to dict for group suppression members
    """
    if isinstance(record, str):
        record = {PK_FIELDS[stream.tap_stream_id][0]: record}

    return record


def send_selected_properties(schema, record, stream, added_properties):
    """
    Creates and returns new record with selected properties.
    Schema is already filtered to only include selected fields by sync().
    """
    r = make_record_if_str(record, stream)

    # Schema has already been filtered, so include all properties
    record = {
        field: r.get(field) for field in schema.to_dict()['properties'].keys()
    }

    if added_properties:
        record.update(added_properties)

    return record


def trimmed_records(schema, data, stream, added_properties):
    """
    Takes raw data and details on what to sync and returns cleaned to records
    with only selected fields
    """
    if data is None:
        return []
    return [send_selected_properties(schema, r, stream, added_properties)
            for r in data]


def get_added_properties(stream, id):
    return {"%s_id" % stream.tap_stream_id.split('_')[0][:-1]: id}


def trim_members_all(tap_stream_id):
    """
    E.g. returns groups for groups_all
    """
    return tap_stream_id.split('_')[0]


def add_all(tap_stream_id):
    """
    Adds all to the generic term e.g. groups_all for groups
    """
    return tap_stream_id.split('-')[0] + '_all'


def find_old_list_count(list_id, all_lists_state):
    """
    Returns the last list size saved for the provided list
    :param list_id:
    :param all_lists_state:
    :return:
    """
    last_size = 0
    for x in all_lists_state:
        if x['id'] == list_id:
            last_size = x['member_count']

    return last_size


def clean_for_cache(data, tap_stream_id):
    """
    For saving lists sizes to cache, clean to just ID and member count.
    Applicable to GROUPS and LISTS (SEGMENTS no longer tracked for member_count)
    """
    lookup_keys = {
        IDS.LISTS_ALL: 'contact_count',
        IDS.GROUPS_ALL: 'unsubscribes',
    }
    if tap_stream_id in lookup_keys:
        return [
            {
                'id': d['id'],
                'member_count': d[lookup_keys[tap_stream_id]]
            } for d in data
        ]
    else:
        return data


def safe_update_dict(obj1, obj2):
    if obj2:
        obj1.update(obj2)
    return obj1


def get_tap_stream_tuple(tap_stream_id):
    for s in STREAMS:
        if s.tap_stream_id == tap_stream_id:
            return s


def write_metrics(tap_stream_id, records):
    with singer.metrics.record_counter(tap_stream_id) as counter:
        counter.increment(len(records))


def write_records(tap_stream_id, records):
    singer.write_records(tap_stream_id, records)
    write_metrics(tap_stream_id, records)
