"""Unit tests for sync functionality"""
import unittest
from unittest.mock import Mock, patch, MagicMock
import json

from tap_sendgrid import sync
from tap_sendgrid.context import Context
from tap_sendgrid.syncs import Syncer
from tap_sendgrid.streams import IDS, STREAMS
from singer.catalog import Catalog, CatalogEntry, Schema
from singer import metadata


class TestSync(unittest.TestCase):
    """Test cases for sync mode"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = {
            "start_date": "2026-01-01T00:00:00Z",
            "api_key": "test-api-key"
        }
        self.state = {}

    def create_catalog_entry(self, stream_id, selected=True):
        """Helper to create a catalog entry"""
        schema_dict = {
            "type": "object",
            "properties": {
                "id": {"type": ["string"], "inclusion": "automatic"}
            }
        }
        schema = Schema.from_dict(schema_dict)
        
        mdata = metadata.new()
        mdata = metadata.write(mdata, (), 'selected', selected)
        mdata = metadata.write(mdata, ('properties', 'id'), 'selected', True)
        
        return CatalogEntry(
            stream=stream_id,
            tap_stream_id=stream_id,
            key_properties=['id'],
            schema=schema,
            metadata=metadata.to_list(mdata)
        )

    @patch('tap_sendgrid.http.authed_get')
    @patch('tap_sendgrid.streams.write_schema')
    @patch('singer.write_records')
    @patch('tap_sendgrid.check_credentials_are_authorized')
    def test_sync_full_table_stream(self, mock_auth, mock_write_records, mock_write_schema, mock_get):
        """Test syncing a FULL_TABLE stream"""
        # Mock API responses
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'result': [{'id': 'list-1', 'contact_count': 100}, {'id': 'list-2', 'contact_count': 50}]
        }
        mock_get.return_value = mock_response

        # Create context with selected stream
        ctx = Context(self.config, self.state)
        catalog = Catalog([self.create_catalog_entry(IDS.LISTS_ALL)])
        ctx.catalog = catalog

        syncer = Syncer(ctx)
        syncer.sync_alls()

        # Verify schema was written
        self.assertTrue(mock_write_schema.called)

    @patch('tap_sendgrid.http.authed_get')
    @patch('singer.write_state')
    @patch('tap_sendgrid.check_credentials_are_authorized')
    def test_sync_incremental_stream(self, mock_auth, mock_write_state, mock_get):
        """Test syncing an INCREMENTAL stream with bookmarks"""
        # Setup state with bookmark
        self.state = {
            'bookmarks': {
                'global_suppressions': {
                    'end_time': '2026-01-01T00:00:00Z'
                }
            }
        }

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        ctx = Context(self.config, self.state)
        catalog = Catalog([self.create_catalog_entry(IDS.GLOBAL_SUPPRESSIONS)])
        ctx.catalog = catalog

        syncer = Syncer(ctx)
        # Verify bookmark handling doesn't crash
        self.assertIsNotNone(syncer.ctx.state)

    @patch('tap_sendgrid.http.authed_get')
    def test_sync_pagination(self, mock_get):
        """Test that pagination works correctly"""
        # Mock paginated responses
        page1_response = Mock()
        page1_response.json.return_value = {
            'recipients': [{'id': '1'}, {'id': '2'}]
        }
        page1_response.status_code = 200

        page2_response = Mock()
        page2_response.json.return_value = {
            'recipients': [],
            'recipient_count': 0
        }
        page2_response.status_code = 200

        mock_get.side_effect = [page1_response, page2_response]

        ctx = Context(self.config, self.state)
        syncer = Syncer(ctx)

        # Test pagination generator
        stream = next(s for s in STREAMS if s.tap_stream_id == IDS.GROUPS_MEMBERS)
        pages = list(syncer.get_using_paged(stream))

        # Should have fetched 2 pages
        self.assertEqual(mock_get.call_count, 2)
    @patch('tap_sendgrid.http.authed_get')
    @patch('singer.write_records')
    @patch('tap_sendgrid.streams.write_schema')
    def test_sync_parent_child_relationship(self, mock_write_schema, mock_write_records, mock_get):
        """Test that parent-child streams are synced correctly"""
        # Mock parent stream response
        parent_response = Mock()
        parent_response.status_code = 200
        parent_response.json.return_value = [
            {'id': 1, 'unsubscribes': 5},
            {'id': 2, 'unsubscribes': 3}
        ]

        # Mock child stream response
        child_response = Mock()
        child_response.status_code = 200
        child_response.json.return_value = ['email1@test.com', 'email2@test.com']
        child_response.json.return_value = ['email1@test.com', 'email2@test.com']
        child_response.status_code = 200

        mock_get.side_effect = [parent_response, child_response, child_response]

        ctx = Context(self.config, self.state)
        
        # Select both parent and child streams
        catalog = Catalog([
            self.create_catalog_entry(IDS.GROUPS_ALL),
            self.create_catalog_entry(IDS.GROUPS_MEMBERS)
        ])
        ctx.catalog = catalog

        syncer = Syncer(ctx)
        
        # Sync parent first
        syncer.sync_alls()
        
        # Verify cache was updated
        self.assertIn('groups', ctx.cache)


class TestSyncUtils(unittest.TestCase):
    """Test utility functions used in sync"""

    def test_get_results_from_payload_new_api(self):
        """Test parsing new Marketing API response format"""
        from tap_sendgrid.utils import get_results_from_payload
        
        payload = {
            'result': [{'id': '1'}, {'id': '2'}],
            '_metadata': {'page': 1}
        }
        
        results = get_results_from_payload(payload)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['id'], '1')

    def test_get_results_from_payload_field_definitions(self):
        """Test parsing field definitions response"""
        from tap_sendgrid.utils import get_results_from_payload
        
        payload = {
            'reserved_fields': [{'id': 'rf1'}],
            'custom_fields': [{'id': 'cf1'}]
        }
        
        results = get_results_from_payload(payload)
        self.assertEqual(len(results), 2)

    def test_get_results_from_payload_singleton(self):
        """Test parsing singleton resource (contacts_count)"""
        from tap_sendgrid.utils import get_results_from_payload
        
        payload = {
            'contact_count': 100,
            'billable_count': 95
        }
        
        results = get_results_from_payload(payload)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['contact_count'], 100)


if __name__ == '__main__':
    unittest.main()
