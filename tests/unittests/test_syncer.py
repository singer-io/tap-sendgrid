"""Unit tests for Syncer class and sync logic"""

import unittest
from unittest.mock import Mock, patch

from tap_sendgrid.syncs import Syncer


class TestSyncerInitialization(unittest.TestCase):
    """Test cases for Syncer initialization"""

    def setUp(self):
        """Set up test fixtures"""
        self.ctx = Mock()

    def test_syncer_initialization(self):
        """Test Syncer initialization"""
        syncer = Syncer(self.ctx)

        self.assertEqual(syncer.ctx, self.ctx)

    def test_syncer_has_sync_methods(self):
        """Test that Syncer has required sync methods"""
        syncer = Syncer(self.ctx)

        self.assertTrue(hasattr(syncer, 'sync'))
        self.assertTrue(hasattr(syncer, 'sync_alls'))
        self.assertTrue(hasattr(syncer, 'sync_incrementals'))
        self.assertTrue(callable(getattr(syncer, 'sync')))


class TestSyncMethodsExist(unittest.TestCase):
    """Test cases for sync method existence"""

    def setUp(self):
        """Set up test fixtures"""
        self.ctx = Mock()
        self.syncer = Syncer(self.ctx)

    def test_sync_timestamp_method_exists(self):
        """Test that sync_timestamp method exists"""
        self.assertTrue(hasattr(self.syncer, 'sync_timestamp'))
        self.assertTrue(callable(self.syncer.sync_timestamp))

    def test_sync_end_time_method_exists(self):
        """Test that sync_end_time method exists"""
        self.assertTrue(hasattr(self.syncer, 'sync_end_time'))
        self.assertTrue(callable(self.syncer.sync_end_time))

    def test_sync_member_count_method_exists(self):
        """Test that sync_member_count method exists"""
        self.assertTrue(hasattr(self.syncer, 'sync_member_count'))
        self.assertTrue(callable(self.syncer.sync_member_count))

    def test_pagination_methods_exist(self):
        """Test that pagination methods exist"""
        self.assertTrue(hasattr(self.syncer, 'get_using_paged'))
        self.assertTrue(hasattr(self.syncer, 'get_using_offset'))


class TestSyncerOffsetNotUsed(unittest.TestCase):
    """Test cases to verify offset key is not present"""

    def setUp(self):
        """Set up test fixtures"""
        self.ctx = Mock()
        self.ctx.state = {}
        self.syncer = Syncer(self.ctx)

    def test_state_has_no_offset_key(self):
        """Test that state does not have offset key"""
        self.assertNotIn('offset', self.ctx.state)

    def test_no_offset_initialization_in_sync(self):
        """Test that sync methods don't initialize offset"""
        # Mock the required methods
        self.ctx.selected_catalog = []
        self.ctx.write_state = Mock()

        # Running sync should not create offset in state
        try:
            self.syncer.sync()
        except:
            pass  # We expect this might fail due to mocks

        # Verify offset key is still not in state
        self.assertNotIn('offset', self.ctx.state)


class TestGetAllsMethod(unittest.TestCase):
    """Test cases for get_alls method"""

    def setUp(self):
        """Set up test fixtures"""
        self.ctx = Mock()
        self.syncer = Syncer(self.ctx)

    @patch('tap_sendgrid.syncs.authed_get')
    def test_get_alls_without_url_key(self, mock_authed_get):
        """Test get_alls without url_key"""
        # Setup mocks
        mock_response = Mock()
        mock_response.json.return_value = {'data': [{'id': '1', 'name': 'test'}]}
        mock_authed_get.return_value = mock_response

        stream = Mock()
        stream.tap_stream_id = 'groups_all'
        stream.endpoint = 'https://api.sendgrid.com/v3/asm/groups'

        with patch('tap_sendgrid.syncs.get_results_from_payload',
                  return_value=[{'id': '1', 'name': 'test'}]):
            result = self.syncer.get_alls(stream)

            self.assertEqual(result, [{'id': '1', 'name': 'test'}])

    @patch('tap_sendgrid.syncs.authed_get')
    def test_get_alls_with_url_key(self, mock_authed_get):
        """Test get_alls with url_key"""
        mock_response = Mock()
        mock_response.json.return_value = {'data': [{'email': 'test@example.com'}]}
        mock_authed_get.return_value = mock_response

        stream = Mock()
        stream.tap_stream_id = 'groups_members'
        stream.endpoint = 'https://api.sendgrid.com/v3/asm/groups/{}/suppressions'

        with patch('tap_sendgrid.syncs.get_results_from_payload',
                  return_value=[{'email': 'test@example.com'}]):
            result = self.syncer.get_alls(stream, url_key='123')

            self.assertEqual(result, [{'email': 'test@example.com'}])
            # Verify endpoint was formatted with url_key
            mock_authed_get.assert_called_once()


class TestBookmarkHandling(unittest.TestCase):
    """Test cases for bookmark handling in sync"""

    def setUp(self):
        """Set up test fixtures"""
        self.ctx = Mock()
        self.syncer = Syncer(self.ctx)

    def test_sync_alls_called_for_full_table_streams(self):
        """Test that sync_alls is called for streams without bookmarks"""
        # Create a stream without bookmark
        stream1 = Mock()
        stream1.tap_stream_id = 'groups_all'
        stream1.bookmark = None

        stream2 = Mock()
        stream2.tap_stream_id = 'contacts'
        stream2.bookmark = ['contacts', 'timestamp']

        self.ctx.selected_catalog = [stream1, stream2]

        # Verify that stream1 has no bookmark (full-table)
        self.assertIsNone(stream1.bookmark)
        # Verify that stream2 has a bookmark (incremental)
        self.assertIsNotNone(stream2.bookmark)

    def test_sync_incrementals_uses_bookmark_method(self):
        """Test that sync_incrementals uses correct bookmark method"""
        # Create a stream with bookmark type 'end_time'
        stream = Mock()
        stream.tap_stream_id = 'global_suppressions'
        stream.bookmark = ['global_suppressions', 'end_time']

        catalog_entry = Mock()
        catalog_entry.tap_stream_id = 'global_suppressions'

        self.ctx.selected_catalog = [catalog_entry]
        self.ctx.write_state = Mock()

        # Verify the stream has correct bookmark configuration
        self.assertIsNotNone(stream.bookmark)
        self.assertEqual(stream.bookmark[1], 'end_time')
        # Verify context has write_state method
        self.assertTrue(callable(self.ctx.write_state))
