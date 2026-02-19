"""Unit tests for discovery functionality"""
import unittest
from unittest.mock import Mock, patch, MagicMock
import json

from tap_sendgrid import discover, streams
from tap_sendgrid.context import Context


class TestDiscovery(unittest.TestCase):
    """Test cases for discovery mode"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = {
            "start_date": "2026-01-01T00:00:00Z",
            "api_key": "test-api-key"
        }
        self.state = {}

    @patch('tap_sendgrid.http.authed_get')
    def test_discovery_generates_catalog(self, mock_get):
        """Test that discovery generates a valid catalog"""
        # Mock the auth check
        mock_response = Mock()
        mock_response.json.return_value = {'scopes': streams.Scopes.scopes}
        mock_get.return_value = mock_response

        ctx = Context(self.config, self.state)
        catalog = discover(ctx)

        # Verify catalog has streams
        self.assertIsNotNone(catalog)
        self.assertEqual(len(catalog.streams), 15)

    @patch('tap_sendgrid.http.authed_get')
    def test_discovery_includes_all_stream_ids(self, mock_get):
        """Test that all expected streams are in the catalog"""
        mock_response = Mock()
        mock_response.json.return_value = {'scopes': streams.Scopes.scopes}
        mock_get.return_value = mock_response

        ctx = Context(self.config, self.state)
        catalog = discover(ctx)

        expected_streams = [
            'global_suppressions', 'groups_all', 'groups_members',
            'lists_all', 'segments_all', 'templates_all',
            'invalids', 'bounces', 'blocks', 'spam_reports',
            'campaigns', 'marketing_contacts_count',
            'marketing_field_definitions', 'marketing_stats_singlesends',
            'senders'
        ]

        catalog_stream_ids = [s.tap_stream_id for s in catalog.streams]
        for expected in expected_streams:
            self.assertIn(expected, catalog_stream_ids)

    @patch('tap_sendgrid.http.authed_get')
    def test_discovery_sets_correct_replication_method(self, mock_get):
        """Test that streams have correct replication methods"""
        mock_response = Mock()
        mock_response.json.return_value = {'scopes': streams.Scopes.scopes}
        mock_get.return_value = mock_response

        ctx = Context(self.config, self.state)
        catalog = discover(ctx)

        # Streams with bookmarks should be INCREMENTAL
        incremental_streams = ['global_suppressions', 'groups_members', 
                              'invalids', 'bounces', 'blocks', 'spam_reports']
        
        for stream in catalog.streams:
            mdata = {entry['breadcrumb']: entry['metadata'] 
                    for entry in stream.metadata}
            
            # Get replication method from top-level metadata
            top_level_mdata = mdata.get(tuple(), {})
            replication_method = top_level_mdata.get('forced-replication-method')
            
            if stream.tap_stream_id in incremental_streams:
                self.assertEqual(replication_method, 'INCREMENTAL',
                               f"Stream {stream.tap_stream_id} should be INCREMENTAL")
            else:
                self.assertEqual(replication_method, 'FULL_TABLE',
                               f"Stream {stream.tap_stream_id} should be FULL_TABLE")

    @patch('tap_sendgrid.http.authed_get')
    def test_discovery_sets_parent_streams(self, mock_get):
        """Test that parent-child relationships are set correctly"""
        mock_response = Mock()
        mock_response.json.return_value = {'scopes': streams.Scopes.scopes}
        mock_get.return_value = mock_response

        ctx = Context(self.config, self.state)
        catalog = discover(ctx)

        # groups_members should have groups_all as parent
        groups_members = next(s for s in catalog.streams 
                            if s.tap_stream_id == 'groups_members')
        
        mdata = {entry['breadcrumb']: entry['metadata'] 
                for entry in groups_members.metadata}
        
        self.assertEqual(mdata[()].get('parent-tap-stream-id'), 'groups_all')

    @patch('tap_sendgrid.http.authed_get')
    def test_discovery_validates_schemas(self, mock_get):
        """Test that all streams have valid schemas"""
        mock_response = Mock()
        mock_response.json.return_value = {'scopes': streams.Scopes.scopes}
        mock_get.return_value = mock_response

        ctx = Context(self.config, self.state)
        catalog = discover(ctx)

        for stream in catalog.streams:
            # Schema should have properties
            self.assertIsNotNone(stream.schema)
            self.assertIn('properties', stream.schema.to_dict())
            
            # Key properties should be defined
            self.assertIsNotNone(stream.key_properties)

    @patch('tap_sendgrid.http.authed_get')
    def test_discovery_fails_with_insufficient_auth(self, mock_get):
        """Test that discovery fails when API key lacks required scopes"""
        # Mock missing scopes
        mock_response = Mock()
        mock_response.json.return_value = {'scopes': ['suppression.read']}
        mock_get.return_value = mock_response

        ctx = Context(self.config, self.state)
        
        with self.assertRaises(Exception) as context:
            discover(ctx)
        
        self.assertIn('Insufficient authorization', str(context.exception))


if __name__ == '__main__':
    unittest.main()
