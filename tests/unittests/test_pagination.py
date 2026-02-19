"""Unit tests for pagination logic"""
import unittest
from unittest.mock import Mock, patch

from tap_sendgrid.syncs import Syncer
from tap_sendgrid.context import Context
from tap_sendgrid.streams import STREAMS, IDS


class TestPagination(unittest.TestCase):
    """Test cases for pagination logic"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = {
            "start_date": "2026-01-01T00:00:00Z",
            "api_key": "test-api-key"
        }
        self.state = {}
        self.ctx = Context(self.config, self.state)

    @patch('tap_sendgrid.http.authed_get')
    def test_pagination_multiple_pages(self, mock_get):
        """Test that multiple pages are fetched correctly"""
        # Mock 3 pages of results
        page1 = Mock()
        page1.json.return_value = {
            'recipients': [{'id': '1'}, {'id': '2'}]
        }
        page1.status_code = 200

        page2 = Mock()
        page2.json.return_value = {
            'recipients': [{'id': '3'}, {'id': '4'}]
        }
        page2.status_code = 200

        page3 = Mock()
        page3.json.return_value = {
            'recipients': [],
            'recipient_count': 0
        }
        page3.status_code = 200

        mock_get.side_effect = [page1, page2, page3]

        syncer = Syncer(self.ctx)
        stream = next(s for s in STREAMS if s.tap_stream_id == IDS.GROUPS_MEMBERS)
        
        pages = list(syncer.get_using_paged(stream))

        # Should have fetched 3 pages
        self.assertEqual(len(pages), 3)
        self.assertEqual(mock_get.call_count, 3)

    @patch('tap_sendgrid.http.authed_get')
    def test_pagination_single_page(self, mock_get):
        """Test that single page results work correctly"""
        page1 = Mock()
        page1.json.return_value = {
            'recipients': [],
            'recipient_count': 0
        }
        page1.status_code = 200

        mock_get.return_value = page1

        syncer = Syncer(self.ctx)
        stream = next(s for s in STREAMS if s.tap_stream_id == IDS.GROUPS_MEMBERS)
        
        pages = list(syncer.get_using_paged(stream))

        # Should have fetched only 1 page
        self.assertEqual(len(pages), 1)
        self.assertEqual(mock_get.call_count, 1)

    @patch('tap_sendgrid.http.authed_get')
    def test_pagination_with_params(self, mock_get):
        """Test that additional params are passed correctly"""
        page1 = Mock()
        page1.json.return_value = {
            'recipients': [],
            'recipient_count': 0
        }
        page1.status_code = 200

        mock_get.return_value = page1

        syncer = Syncer(self.ctx)
        stream = next(s for s in STREAMS if s.tap_stream_id == IDS.GROUPS_MEMBERS)
        
        list(syncer.get_using_paged(stream, add_params={'filter': 'test'}))

        # Check that custom params were merged with pagination params
        call_args = mock_get.call_args
        params = call_args[1]['params']
        self.assertIn('page', params)
        self.assertIn('page_size', params)
        self.assertEqual(params['filter'], 'test')

    @patch('tap_sendgrid.http.authed_get')
    def test_pagination_404_empty_message(self, mock_get):
        """Test that 404 with 'No more pages' message stops pagination"""
        page1 = Mock()
        page1.json.return_value = {
            'recipients': [{'id': '1'}]
        }
        page1.status_code = 200

        page2 = Mock()
        page2.json.return_value = {
            'errors': [{'message': 'No more pages'}]
        }
        page2.status_code = 404

        mock_get.side_effect = [page1, page2]

        syncer = Syncer(self.ctx)
        stream = next(s for s in STREAMS if s.tap_stream_id == IDS.GROUPS_MEMBERS)
        
        pages = list(syncer.get_using_paged(stream))

        # Should have fetched 2 pages and stopped
        self.assertEqual(len(pages), 2)

    @patch('tap_sendgrid.http.authed_get')
    def test_offset_pagination(self, mock_get):
        """Test offset-based pagination for incremental streams"""
        # Mock offset pagination responses
        page1 = Mock()
        page1.json.return_value = [{'email': 'test1@example.com'}]

        page2 = Mock()
        page2.json.return_value = []

        mock_get.side_effect = [page1, page2]

        syncer = Syncer(self.ctx)
        stream = next(s for s in STREAMS if s.tap_stream_id == IDS.GLOBAL_SUPPRESSIONS)
        
        start = 1609459200  # 2021-01-01 timestamp
        end = 1640995200    # 2022-01-01 timestamp
        
        pages = list(syncer.get_using_offset(stream, start, end))

        # Should have fetched 2 pages
        self.assertEqual(len(pages), 2)
        
        # Verify offset incremented
        first_call_params = mock_get.call_args_list[0][1]['params']
        second_call_params = mock_get.call_args_list[1][1]['params']
        
        self.assertEqual(first_call_params['offset'], 0)
        self.assertEqual(second_call_params['offset'], 500)


if __name__ == '__main__':
    unittest.main()
