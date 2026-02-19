"""GroupsMembers stream for SendGrid tap."""
from tap_sendgrid.streams.abstracts import ChildStream


class GroupsMembers(ChildStream):
    """Stream for retrieving suppression group members."""
    tap_stream_id = "groups_members"
    key_properties = ["email", "group_id"]
    replication_method = "INCREMENTAL"
    replication_keys = []
    parent = "groups_all"
    path = "asm/groups/{id}/suppressions"
    data_key = "result"

    def modify_object(self, record, parent_record=None):
        """Transform email string to dict and include group_id from parent."""
        if isinstance(record, str):
            record = {"email": record}
        if parent_record and "group_id" not in record:
            record["group_id"] = parent_record.get("id")
        return record
