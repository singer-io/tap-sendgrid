"""Abstract base classes for tap-sendgrid stream implementations.

Defines ``BaseStream`` and four concrete mix-ins/subclasses:
``OffsetPagedStream``, ``CursorPagedStream``, ``IncrementalStream``,
and ``FullTableStream``.
"""
from abc import ABC, abstractmethod
import copy
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from singer import (
    Transformer,
    get_bookmark,
    metadata,
    metrics,
    write_bookmark,
    write_record,
    write_schema,
)

from tap_sendgrid.client import to_unix_timestamp


class BaseStream(ABC):
    """Abstract base class shared by all tap-sendgrid stream implementations."""

    path = ""
    data_key = None
    page_size = 500
    page_size_param = "limit"
    page_offset_param = "offset"
    cursor_path = "_metadata.next"
    parent = ""
    children = []

    def __init__(self, client=None, catalog=None) -> None:
        """Initialise the stream with a ``Client`` instance and a catalog entry."""
        self.client = client
        self.catalog = catalog
        self.schema = catalog.schema.to_dict()
        self.metadata = metadata.to_map(catalog.metadata)
        self.child_to_sync = []

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        """Unique stream identifier used in Singer messages and catalog."""

    @property
    @abstractmethod
    def replication_method(self) -> str:
        """Replication strategy: ``FULL_TABLE`` or ``INCREMENTAL``."""

    @property
    @abstractmethod
    def replication_keys(self) -> Tuple[str, ...]:
        """Tuple of field names used as the replication cursor for incremental streams."""

    @property
    @abstractmethod
    def key_properties(self) -> Tuple[str, ...]:
        """Tuple of field names that uniquely identify a record."""

    def is_selected(self) -> bool:
        """Return ``True`` when this stream is selected in the catalog."""
        return bool(metadata.get(self.metadata, (), "selected"))

    def write_schema(self) -> None:
        """Emit a Singer SCHEMA message for this stream."""
        write_schema(self.tap_stream_id, self.schema, list(self.key_properties))

    def get_path(self, parent_obj: Optional[Dict] = None) -> str:
        """Return the API path, formatting a parent ID placeholder when present."""
        if parent_obj and "{}" in self.path:
            return self.path.format(parent_obj["id"])
        return self.path

    def parse_records(self, response: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        """Extract the record list from an API *response* payload."""
        if self.data_key is None:
            if isinstance(response, list):
                return response
            return []
        return response.get(self.data_key) or []

    def next_page_params(self, _response: Dict[str, Any]) -> Dict[str, Any]:
        """Return pagination parameters for the next page; empty dict means no more pages."""
        return {}

    def get_records(
        self,
        params: Optional[Dict[str, Any]] = None,
        parent_obj: Optional[Dict[str, Any]] = None,
    ) -> Iterable[Dict[str, Any]]:
        """Iterate over all pages of records, yielding each record dict."""
        current_params = dict(params or {})
        max_pages = 10_000
        page_count = 0
        has_more_pages = True
        while has_more_pages and page_count < max_pages:
            page_count += 1
            response = self.client.get(
                self.get_path(parent_obj),
                params=current_params,
                stream_name=self.tap_stream_id,
            )
            records = self.parse_records(response)
            yield from records
            next_params = self.next_page_params(response)
            if not next_params:
                has_more_pages = False
            else:
                current_params.update(next_params)

    @abstractmethod
    def sync(self, state: Dict, transformer: Transformer, parent_obj: Optional[Dict] = None) -> int:
        """Sync records, write Singer messages, and return the record count."""


class OffsetPagedStream(BaseStream):
    """Stream base class that paginates via an integer offset parameter."""

    def next_page_params(self, response: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Return offset-based params for the next page, or ``None`` when exhausted."""
        if not isinstance(response, dict):
            return None
        current_offset = int(response.get("offset", 0))
        page_size = int(response.get("limit", self.page_size))
        records = list(self.parse_records(response))
        if len(records) < page_size:
            return None
        return {self.page_offset_param: current_offset + page_size}


class CursorPagedStream(BaseStream):
    """Stream base class that paginates via a cursor URL embedded in the response metadata."""

    def next_page_params(self, response: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse the ``_metadata.next`` URL and return its query params, or ``None``."""
        if not isinstance(response, dict):
            return None
        metadata_obj = response.get("_metadata", {})
        next_url = metadata_obj.get("next")
        if not next_url:
            return None
        parsed = urlparse(next_url)
        query_params = parse_qs(parsed.query)
        out = {}
        for key, value in query_params.items():
            if value:
                out[key] = value[0]
        return out


class IncrementalStream(OffsetPagedStream):
    """Stream base class for incremental replication keyed on a datetime or unix cursor."""

    cursor_type = "datetime"

    def get_start_value(self, state: Dict) -> Any:
        """Return the bookmark value (or config start_date) to begin replication from."""
        return get_bookmark(
            state,
            self.tap_stream_id,
            self.replication_keys[0],
            self.client.config["start_date"],
        )

    def get_params_for_sync(self, bookmark: Any) -> Dict[str, Any]:
        """Build the initial query params dict based on *bookmark* and tap config."""
        params = {
            self.page_size_param: int(self.client.config.get("page_size", self.page_size)),
            self.page_offset_param: 0,
        }
        lookback_days = int(self.client.config.get("lookback_window_days", 0))
        if self.cursor_type == "unix":
            start_time = int(bookmark)
            if lookback_days:
                start_time = max(0, start_time - (lookback_days * 24 * 60 * 60))
            params["start_time"] = start_time
        return params

    def normalize_record_cursor(self, record: Dict[str, Any]) -> Any:
        """Coerce the replication key value to the expected type and update *record* in place."""
        key = self.replication_keys[0]
        value = record.get(key)
        if self.cursor_type == "unix" and isinstance(value, str):
            value = int(value)
            record[key] = value
        return value

    def sync(self, state: Dict, transformer: Transformer, parent_obj: Optional[Dict] = None) -> int:
        """Sync incremental records, filter by bookmark, and persist state."""
        bookmark = self.get_start_value(state)
        if self.cursor_type == "unix" and isinstance(bookmark, str):
            bookmark = to_unix_timestamp(bookmark)
        current_max = bookmark
        record_count = 0

        with metrics.record_counter(self.tap_stream_id) as counter:
            params = self.get_params_for_sync(bookmark)
            for record in self.get_records(params=params, parent_obj=parent_obj):
                transformed_record = transformer.transform(
                    copy.deepcopy(record), self.schema, self.metadata
                )
                record_value = self.normalize_record_cursor(transformed_record)
                if record_value is None or record_value < bookmark:
                    continue

                if self.is_selected():
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()
                    record_count += 1

                current_max = max(current_max, record_value)
                for child in self.child_to_sync:
                    child.sync(state=state, transformer=transformer, parent_obj=record)

        if self.cursor_type == "unix":
            bk_str = datetime.fromtimestamp(current_max, tz=timezone.utc).isoformat()
        else:
            bk_str = str(current_max)
        write_bookmark(state, self.tap_stream_id, self.replication_keys[0], bk_str)
        return record_count


class FullTableStream(CursorPagedStream):
    """Stream base class for full-table (non-incremental) replication."""

    def default_params(self) -> Dict[str, Any]:
        """Return the default query params, including the configured page size."""
        return {"page_size": int(self.client.config.get("page_size", 50))}

    def sync(self, state: Dict, transformer: Transformer, parent_obj: Optional[Dict] = None) -> int:
        """Sync all records for a full-table stream and optionally bookmark completion time."""
        record_count = 0
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(params=self.default_params(), parent_obj=parent_obj):
                transformed_record = transformer.transform(
                    copy.deepcopy(record), self.schema, self.metadata
                )
                if self.is_selected():
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()
                    record_count += 1

                for child in self.child_to_sync:
                    child.sync(state=state, transformer=transformer, parent_obj=record)

        if self.replication_keys:
            key = self.replication_keys[0]
            write_bookmark(state, self.tap_stream_id, key, datetime.now(timezone.utc).isoformat())

        return record_count
