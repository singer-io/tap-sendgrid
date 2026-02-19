"""Unit tests for Marketing API v3 streams"""
import unittest
from unittest.mock import Mock, patch

from tap_sendgrid.streams import IDS, STREAMS, Scopes
from tap_sendgrid.utils import get_results_from_payload
from tap_sendgrid.syncs import Syncer
from tap_sendgrid.context import Context


class TestMarketingAPIStreams(unittest.TestCase):
    """Test cases for Marketing API v3 streams"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = {
            "start_date": "2026-01-01T00:00:00Z",
            "api_key": "test-api-key"
        }
        self.state = {}

    def test_marketing_read_scope_included(self):
        """Test that marketing.read scope is included"""
        self.assertIn('marketing.read', Scopes.scopes)

    def test_marketing_streams_defined(self):
        """Test that all Marketing API streams are defined"""
        marketing_streams = [
            IDS.LISTS_ALL,
            IDS.SEGMENTS_ALL,
            IDS.CAMPAIGNS,
            IDS.MARKETING_CONTACTS_COUNT,
            IDS.MARKETING_FIELD_DEFINITIONS,
            IDS.MARKETING_STATS_SINGLESENDS,
            IDS.SENDERS
        ]
        
        stream_ids = [s.tap_stream_id for s in STREAMS]
        for marketing_stream in marketing_streams:
            self.assertIn(marketing_stream, stream_ids)

    def test_lists_endpoint_uses_marketing_api(self):
        """Test that lists_all uses new Marketing API endpoint"""
        stream = next(s for s in STREAMS if s.tap_stream_id == IDS.LISTS_ALL)
        self.assertIn('marketing/lists', stream.endpoint)
        self.assertNotIn('contactdb', stream.endpoint)

    def test_segments_endpoint_uses_marketing_api(self):
        """Test that segments_all uses new Marketing API v2.0 endpoint"""
        stream = next(s for s in STREAMS if s.tap_stream_id == IDS.SEGMENTS_ALL)
        self.assertIn('marketing/segments/2.0', stream.endpoint)
        self.assertNotIn('contactdb', stream.endpoint)

    def test_campaigns_endpoint_uses_singlesends(self):
        """Test that campaigns uses Single Sends API"""
        stream = next(s for s in STREAMS if s.tap_stream_id == IDS.CAMPAIGNS)
        self.assertIn('marketing/singlesends', stream.endpoint)

    def test_new_marketing_streams_endpoints(self):
        """Test that new Marketing API streams have correct endpoints"""
        test_cases = [
            (IDS.MARKETING_CONTACTS_COUNT, 'marketing/contacts/count'),
            (IDS.MARKETING_FIELD_DEFINITIONS, 'marketing/field_definitions'),
            (IDS.MARKETING_STATS_SINGLESENDS, 'marketing/stats/singlesends'),
            (IDS.SENDERS, 'senders')
        ]
        
        for stream_id, expected_path in test_cases:
            stream = next(s for s in STREAMS if s.tap_stream_id == stream_id)
            self.assertIn(expected_path, stream.endpoint)

    def test_deprecated_streams_removed(self):
        """Test that deprecated contactdb streams are not in stream list"""
        deprecated_streams = ['contacts', 'lists_members', 'segments_members']
        stream_ids = [s.tap_stream_id for s in STREAMS]
        
        for deprecated in deprecated_streams:
            self.assertNotIn(deprecated, stream_ids)

    @patch('tap_sendgrid.http.authed_get')
    def test_marketing_contacts_count_response_parsing(self, mock_get):
        """Test parsing of contacts count API response"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'contact_count': 1000,
            'billable_count': 950
        }
        mock_get.return_value = mock_response

        ctx = Context(self.config, self.state)
        syncer = Syncer(ctx)
        stream = next(s for s in STREAMS if s.tap_stream_id == IDS.MARKETING_CONTACTS_COUNT)
        
        results = syncer.get_alls(stream)
        
        # Should wrap singleton in array
        self.assertIsInstance(results, list)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['contact_count'], 1000)

    @patch('tap_sendgrid.http.authed_get')
    def test_field_definitions_response_parsing(self, mock_get):
        """Test parsing of field definitions API response"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'reserved_fields': [
                {'id': '_rf1_T', 'name': 'email'},
                {'id': '_rf2_T', 'name': 'first_name'}
            ],
            'custom_fields': [
                {'id': 'cf1', 'name': 'custom_field'}
            ]
        }
        mock_get.return_value = mock_response

        ctx = Context(self.config, self.state)
        syncer = Syncer(ctx)
        stream = next(s for s in STREAMS if s.tap_stream_id == IDS.MARKETING_FIELD_DEFINITIONS)
        
        results = syncer.get_alls(stream)
        
        # Should merge reserved and custom fields
        self.assertIsInstance(results, list)
        self.assertEqual(len(results), 3)

    @patch('tap_sendgrid.http.authed_get')
    def test_lists_new_api_response_format(self, mock_get):
        """Test parsing of new Lists API response format"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'result': [
                {'id': 'list-1', 'name': 'List 1', 'contact_count': 100},
                {'id': 'list-2', 'name': 'List 2', 'contact_count': 50}
            ],
            '_metadata': {
                'page': 1,
                'total_count': 2
            }
        }
        mock_get.return_value = mock_response

        ctx = Context(self.config, self.state)
        syncer = Syncer(ctx)
        stream = next(s for s in STREAMS if s.tap_stream_id == IDS.LISTS_ALL)
        
        results = syncer.get_alls(stream)
        
        # Should extract 'result' array
        self.assertIsInstance(results, list)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['id'], 'list-1')

    def test_marketing_api_result_extraction(self):
        """Test get_results_from_payload handles Marketing API format"""
        # Test new Marketing API format with result
        payload = {
            'result': [{'id': '1'}, {'id': '2'}],
            '_metadata': {'page': 1}
        }
        results = get_results_from_payload(payload)
        self.assertEqual(len(results), 2)

        # Test field definitions format
        payload = {
            'reserved_fields': [{'id': 'rf1'}],
            'custom_fields': [{'id': 'cf1'}]
        }
        results = get_results_from_payload(payload)
        self.assertEqual(len(results), 2)

        # Test singleton resource
        payload = {
            'contact_count': 100,
            'billable_count': 95
        }
        results = get_results_from_payload(payload)
        self.assertEqual(len(results), 1)


if __name__ == '__main__':
    unittest.main()
