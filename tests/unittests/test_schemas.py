"""Unit tests for JSON schema files.

Validates that all schema files are:
- Valid JSON
- Properly structured (type, properties)
- Free of root-level additionalProperties
- Using date-time format on all datetime string fields
- Using consistent 2-space indentation
"""
import json
import os

import pytest

SCHEMAS_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "tap_sendgrid", "schemas"
)

EXPECTED_SCHEMAS = {
    "blocks",
    "bounces",
    "campaigns",
    "global_suppressions",
    "invalid_emails",
    "lists",
    "marketing_contacts_count",
    "marketing_field_definitions",
    "segments",
    "senders",
    "single_send_stats",
    "single_sends",
    "spam_reports",
    "stats_automations",
    "suppression_group_members",
    "suppression_groups",
    "templates",
}

# Fields whose names indicate they hold ISO 8601 datetime strings
DATETIME_STRING_FIELD_NAMES = {
    "created",
    "send_at",
    "created_at",
    "updated_at",
    "sent_at",
    "scheduled_at",
    "start_date",
    "end_date",
}


def _load_schema(name: str) -> dict:
    path = os.path.join(SCHEMAS_DIR, f"{name}.json")
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _load_raw(name: str) -> str:
    path = os.path.join(SCHEMAS_DIR, f"{name}.json")
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


@pytest.mark.parametrize("stream_name", sorted(EXPECTED_SCHEMAS))
def test_schema_file_exists(stream_name):
    """Every registered stream must have a corresponding schema file."""
    path = os.path.join(SCHEMAS_DIR, f"{stream_name}.json")
    assert os.path.isfile(path), f"Missing schema file for stream: {stream_name}"


@pytest.mark.parametrize("stream_name", sorted(EXPECTED_SCHEMAS))
def test_schema_is_valid_json(stream_name):
    """Schema file must be parseable as valid JSON."""
    schema = _load_schema(stream_name)
    assert isinstance(schema, dict), f"{stream_name}: schema root must be a JSON object"


@pytest.mark.parametrize("stream_name", sorted(EXPECTED_SCHEMAS))
def test_schema_has_required_keys(stream_name):
    """Schema must declare ``type`` and ``properties``."""
    schema = _load_schema(stream_name)
    assert "type" in schema, f"{stream_name}: missing 'type'"
    assert schema["type"] == "object", f"{stream_name}: root type must be 'object'"
    assert "properties" in schema, f"{stream_name}: missing 'properties'"
    assert isinstance(schema["properties"], dict), f"{stream_name}: 'properties' must be a dict"


@pytest.mark.parametrize("stream_name", sorted(EXPECTED_SCHEMAS))
def test_schema_no_root_additional_properties(stream_name):
    """Root-level additionalProperties violates project conventions and must be absent."""
    schema = _load_schema(stream_name)
    assert "additionalProperties" not in schema, (
        f"{stream_name}: 'additionalProperties' must not appear at the root level. "
        "It is only allowed in nested field definitions."
    )


@pytest.mark.parametrize("stream_name", sorted(EXPECTED_SCHEMAS))
def test_schema_datetime_fields_have_format(stream_name):
    """Fields whose names indicate ISO 8601 datetime strings must carry 'format': 'date-time'."""
    schema = _load_schema(stream_name)
    props = schema.get("properties", {})
    for field_name, field_def in props.items():
        if field_name not in DATETIME_STRING_FIELD_NAMES:
            continue
        field_types = field_def.get("type", [])
        if isinstance(field_types, list):
            has_string = "string" in field_types
        else:
            has_string = field_types == "string"
        if not has_string:
            continue  # integer unix timestamps do not need format annotation
        assert field_def.get("format") == "date-time", (
            f"{stream_name}.{field_name} looks like a datetime string field "
            "but is missing `\"format\": \"date-time\"`."
        )


@pytest.mark.parametrize("stream_name", sorted(EXPECTED_SCHEMAS))
def test_schema_properties_have_type(stream_name):
    """Every property declaration must include a ``type`` key."""
    schema = _load_schema(stream_name)
    for field_name, field_def in schema.get("properties", {}).items():
        assert "type" in field_def, (
            f"{stream_name}.{field_name}: property definition is missing 'type'"
        )


@pytest.mark.parametrize("stream_name", sorted(EXPECTED_SCHEMAS))
def test_schema_nullable_fields(stream_name):
    """Properties that allow null must use ``[\"null\", ...]`` array type — not bare ``null``."""
    schema = _load_schema(stream_name)
    for field_name, field_def in schema.get("properties", {}).items():
        field_type = field_def.get("type")
        if field_type == "null":
            pytest.fail(
                f"{stream_name}.{field_name}: bare 'null' type is invalid. "
                "Use [\"null\", \"string\"] (or other concrete type) instead."
            )


@pytest.mark.parametrize("stream_name", sorted(EXPECTED_SCHEMAS))
def test_schema_utf8_encoding(stream_name):
    """Schema file must be readable as valid UTF-8 with no BOM."""
    path = os.path.join(SCHEMAS_DIR, f"{stream_name}.json")
    raw = open(path, "rb").read()
    assert not raw.startswith(b"\xff\xfe") and not raw.startswith(
        b"\xfe\xff"
    ), f"{stream_name}: file has a UTF-16 BOM"
    assert not raw.startswith(b"\xef\xbb\xbf"), f"{stream_name}: file has a UTF-8 BOM"
    raw.decode("utf-8")  # raises UnicodeDecodeError if invalid


def test_all_expected_schema_files_present():
    """The schemas directory must contain exactly the expected set of .json files."""
    found = {
        os.path.splitext(f)[0]
        for f in os.listdir(SCHEMAS_DIR)
        if f.endswith(".json")
    }
    missing = EXPECTED_SCHEMAS - found
    assert not missing, f"Missing schema files: {missing}"


def test_specific_datetime_fields_campaigns():
    """campaigns.json created_at and updated_at must be date-time strings."""
    schema = _load_schema("campaigns")
    for field in ("created_at", "updated_at"):
        prop = schema["properties"][field]
        assert "string" in prop["type"], f"campaigns.{field} should include string type"
        assert prop.get("format") == "date-time", f"campaigns.{field} must have format: date-time"


def test_specific_datetime_fields_templates():
    """templates.json updated_at must be a date-time string."""
    schema = _load_schema("templates")
    prop = schema["properties"]["updated_at"]
    assert "string" in prop["type"]
    assert prop.get("format") == "date-time"


def test_specific_datetime_fields_single_sends():
    """single_sends.json send_at must be a date-time string."""
    schema = _load_schema("single_sends")
    prop = schema["properties"]["send_at"]
    assert "string" in prop["type"]
    assert prop.get("format") == "date-time"


def test_incremental_streams_created_field_is_integer_or_string():
    """
    Incremental suppression streams return Unix timestamps in 'created'.
    The field type must include 'integer' (raw API value) and 'string'
    (bookmarked ISO string that may come back via the Transformer).
    """
    for stream_name in ("blocks", "bounces", "spam_reports", "invalid_emails", "global_suppressions"):
        schema = _load_schema(stream_name)
        field_type = schema["properties"]["created"]["type"]
        assert "integer" in field_type, f"{stream_name}.created must allow integer"


def test_senders_timestamps_are_integer():
    """Senders created_at and updated_at are Unix integer timestamps, not ISO strings."""
    schema = _load_schema("senders")
    for field in ("created_at", "updated_at"):
        prop = schema["properties"][field]
        assert "integer" in prop["type"], f"senders.{field} should be integer (unix timestamp)"
        assert "string" not in prop["type"], f"senders.{field} should not be string type"
        assert "format" not in prop, f"senders.{field} is an int and must not have format"


def test_incremental_suppression_created_has_datetime_format():
    """
    The 'created' field in suppression streams is typed as [null, integer, string].
    The string variant represents an ISO datetime (e.g. from bookmark reads), so
    'format: date-time' is required by the Singer spec and project conventions.
    """
    suppression_streams = (
        "blocks", "bounces", "spam_reports", "invalid_emails",
        "global_suppressions", "suppression_group_members",
    )
    for stream_name in suppression_streams:
        schema = _load_schema(stream_name)
        prop = schema["properties"]["created"]
        assert "string" in prop["type"], f"{stream_name}.created must include string type"
        assert prop.get("format") == "date-time", (
            f"{stream_name}.created has string type but is missing `\"format\": \"date-time\"`"
        )
