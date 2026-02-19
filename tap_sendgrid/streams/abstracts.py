"""Base stream classes for SendGrid tap."""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Iterator
from urllib.parse import urlparse, parse_qs
from singer import (
    Transformer,
    get_bookmark,
    get_logger,
    metrics,
    write_bookmark,
    write_record,
    write_schema,
    metadata
)
from tap_sendgrid.http import authed_get

LOGGER = get_logger()

BASE_URL = "https://api.sendgrid.com/v3"


class BaseStream(ABC):
    """
    A Base Class providing structure and boilerplate for generic streams
    and required attributes for any kind of stream.

    Provides:
     - Basic Attributes (tap_stream_id, replication_method, key_properties)
     - Helper methods for catalog generation
     - `sync` and `get_records` method for performing sync
    """

    path = ""  # URL path relative to BASE_URL
    data_key = "result"  # SendGrid Marketing API v3 uses "result" by default
    parent = None
    children = []

    def __init__(self, client=None, catalog=None, config=None) -> None:
        self.client = client  # Not used in SendGrid (uses global authed_get)
        self.catalog = catalog
        self.config = config or {}
        self.schema = catalog.schema.to_dict() if catalog else {}
        self.metadata = metadata.to_map(catalog.metadata) if catalog else {}
        self.child_to_sync = []
        self.params = {}

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        """Unique identifier for the stream."""

    @property
    @abstractmethod
    def replication_method(self) -> str:
        """Defines the sync mode of a stream (FULL_TABLE or INCREMENTAL)."""

    @property
    @abstractmethod
    def replication_keys(self) -> List:
        """Defines the replication key for incremental sync mode."""

    @property
    @abstractmethod
    def key_properties(self) -> List[str]:
        """List of key properties for stream."""

    def is_selected(self):
        """Check if stream is selected in catalog."""
        return metadata.get(self.metadata, (), "selected")

    @abstractmethod
    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """
        Performs a replication sync for the stream.
        
        Args:
         - state (dict): represents the state file for the tap.
         - transformer (object): A Object of the singer.transformer class.
         - parent_obj (dict): The parent object for the stream.

        Returns:
         - int: The number of records synced
        """

    def get_records(self, config: Dict) -> Iterator:
        """
        Interacts with API and handles cursor-based pagination.

        Used by Marketing API endpoints that return:
          { "result": [...], "_metadata": { "next": "...?page_token=..." } }

        For suppression APIs (offset-based), IncrementalStream overrides this.
        """
        params = self.params.copy()
        endpoint = f"{BASE_URL}/{self.path}"

        has_more = True
        while has_more:
            response = authed_get(
                self.tap_stream_id,
                endpoint,
                config,
                params=params
            )

            data = response.json()

            if isinstance(data, list):
                # Some endpoints (e.g. /asm/groups) return a bare array
                yield from data
                has_more = False
            elif isinstance(data, dict):
                # Standard Marketing API envelope: {"result": [...], "_metadata": {...}}
                results = data.get(self.data_key)
                if results is None:
                    results = data.get('results', data.get('result', []))

                if isinstance(results, list):
                    yield from results
                elif results:
                    yield results

                # Follow cursor-based pagination via _metadata.next
                meta = data.get('_metadata', {}) or {}
                next_url = meta.get('next')
                if next_url and isinstance(next_url, str):
                    parsed = urlparse(next_url)
                    qs = parse_qs(parsed.query)
                    page_token = qs.get('page_token', [None])[0]
                    if page_token:
                        params = self.params.copy()
                        params['page_token'] = page_token
                        has_more = True
                    else:
                        has_more = False
                else:
                    has_more = False
            else:
                has_more = False

    def write_schema(self) -> None:
        """Write a schema message."""
        try:
            write_schema(self.tap_stream_id, self.schema, self.key_properties)
        except OSError as err:
            LOGGER.error(
                "OS Error while writing schema for: %s", self.tap_stream_id
            )
            raise err

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """
        Modify the record before writing to the stream.
        Can be overridden by subclasses for custom transformations.
        """
        _ = parent_record  # Unused but kept for interface consistency
        return record


class FullTableStream(BaseStream):
    """Base Class for Full Table Stream."""

    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Implementation for FULL_TABLE stream."""
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(self.config):
                record = self.modify_object(record, parent_obj)
                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )
                if self.is_selected():
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()

                for child in self.child_to_sync:
                    child.sync(state=state, transformer=transformer, parent_obj=record)

            return counter.value


class IncrementalStream(BaseStream):
    """Base Class for Incremental Stream."""

    replication_method = "INCREMENTAL"

    # Suppression API page size (max 500 per SendGrid docs)
    _PAGE_SIZE = 500

    def get_records(self, config: Dict) -> Iterator:
        """
        Offset-based pagination for SendGrid suppression APIs.

        These endpoints (/suppression/blocks, /suppression/bounces, etc.)
        return a bare JSON array. We page through with limit/offset until
        a page comes back with fewer items than the page size.
        """
        params = self.params.copy()
        params['limit'] = self._PAGE_SIZE
        params['offset'] = 0
        endpoint = f"{BASE_URL}/{self.path}"

        has_more = True
        while has_more:
            response = authed_get(
                self.tap_stream_id,
                endpoint,
                config,
                params=params
            )
            records = response.json()

            if not isinstance(records, list):
                LOGGER.warning(
                    "Unexpected response format for %s: expected list, got %s",
                    self.tap_stream_id, type(records).__name__
                )
                break

            yield from records

            if len(records) < self._PAGE_SIZE:
                has_more = False
            else:
                params['offset'] += self._PAGE_SIZE

    def get_bookmark(self, state: dict, stream: str, key: Any = None) -> str:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        return get_bookmark(
            state,
            stream,
            key or self.replication_keys[0],
            self.config.get("start_date", "2010-01-01T00:00:00Z"),
        )

    def write_bookmark(
        self, state: dict, stream: str, key: Any = None, value: Any = None
    ) -> Dict:
        """A wrapper for singer.write_bookmark."""
        if not (key or self.replication_keys):
            return state

        bookmark_key = key or self.replication_keys[0]
        current_bookmark = get_bookmark(
            state, stream, bookmark_key, self.config.get("start_date", "2010-01-01T00:00:00Z")
        )
        value = max(current_bookmark, value)
        return write_bookmark(state, stream, bookmark_key, value)

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Implementation for INCREMENTAL stream."""
        bookmark_date = self.get_bookmark(state, self.tap_stream_id)
        current_max_bookmark_date = bookmark_date

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(self.config):
                record = self.modify_object(record, parent_obj)
                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )

                # Get bookmark value from record
                record_bookmark = transformed_record.get(self.replication_keys[0], bookmark_date)

                if record_bookmark >= bookmark_date:
                    if self.is_selected():
                        write_record(self.tap_stream_id, transformed_record)
                        counter.increment()

                    current_max_bookmark_date = max(
                        current_max_bookmark_date, record_bookmark
                    )

                    for child in self.child_to_sync:
                        child.sync(state=state, transformer=transformer, parent_obj=record)

            state = self.write_bookmark(state, self.tap_stream_id, value=current_max_bookmark_date)
            return counter.value


class ParentStream(FullTableStream):
    """Base class for parent streams that have children."""

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Implementation for parent stream - syncs self and children."""
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(self.config):
                record = self.modify_object(record, parent_obj)
                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )
                if self.is_selected():
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()

                # Sync children for each parent record
                for child in self.child_to_sync:
                    child.sync(state=state, transformer=transformer, parent_obj=transformed_record)

            return counter.value


class ChildStream(FullTableStream):
    """Base class for child streams that depend on parent stream."""

    def __init__(self, client=None, catalog=None, config=None) -> None:
        """Initialize ChildStream."""
        super().__init__(client, catalog, config)

    def get_endpoint(self, parent_obj: Dict) -> str:
        """Get endpoint URL with parent ID substituted."""
        if not parent_obj:
            raise ValueError(f"Child stream {self.tap_stream_id} requires parent object")
        path = self.path.format(**parent_obj)
        return f"{BASE_URL}/{path}"

    def get_records(self, config: Dict, parent_obj: Dict = None) -> Iterator:
        """Override to use parent-specific endpoint."""
        endpoint = self.get_endpoint(parent_obj)
        params = self.params.copy()

        response = authed_get(
            self.tap_stream_id,
            endpoint,
            config,
            params=params
        )

        data = response.json()

        # Handle different response formats
        if isinstance(data, list):
            yield from data
        elif isinstance(data, dict):
            results = data.get(self.data_key, data.get('results', data.get('result', [])))
            if results:
                if isinstance(results, list):
                    yield from results
                else:
                    yield results

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Implementation for child stream."""
        if not parent_obj:
            LOGGER.warning(f"Skipping {self.tap_stream_id} - no parent object provided")
            return 0

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(self.config, parent_obj):
                record = self.modify_object(record, parent_obj)
                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )
                if self.is_selected():
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()

            return counter.value
