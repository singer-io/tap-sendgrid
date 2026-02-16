"""Unit tests for Context class"""

import unittest
from unittest.mock import Mock, patch

from tap_sendgrid.context import Context


class TestContext(unittest.TestCase):
    """Test cases for Context class"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = {
            "start_date": "2026-02-01",
            "api_key": "test-api-key"
        }
        self.state = {}

    def test_context_initialization(self):
        """Test Context initialization"""
        ctx = Context(self.config, self.state)

        self.assertIsNone(ctx._catalog)
        self.assertIsNone(ctx.selected_stream_ids)
        self.assertIsNone(ctx.selected_catalog)
        self.assertEqual(ctx.config, self.config)
        self.assertEqual(ctx.state, self.state)
        self.assertEqual(ctx.cache, {})

    def test_offset_not_in_state(self):
        """Test that offset key should not be present in initial state"""
        ctx = Context(self.config, self.state)

        # Verify offset is not in state
        self.assertNotIn('offset', ctx.state)
        self.assertEqual(ctx.state, {})

    def test_get_bookmark_with_no_existing_bookmark(self):
        """Test get_bookmark when no bookmark exists"""
        with patch('tap_sendgrid.context.bks_.get_bookmark', return_value=None):
            ctx = Context(self.config, self.state)
            result = ctx.get_bookmark(['stream_id', 'bookmark_key'])

            self.assertIsNone(result)

    def test_update_start_date_bookmark_new(self):
        """Test update_start_date_bookmark for new bookmark"""
        with patch('tap_sendgrid.context.bks_.get_bookmark', return_value=None):
            with patch('tap_sendgrid.context.bks_.write_bookmark') as mock_write:
                with patch('tap_sendgrid.context.singer.write_state'):
                    ctx = Context(self.config, self.state)
                    result = ctx.update_start_date_bookmark(['contacts', 'timestamp'])

                    # Should return parsed start_date from config
                    self.assertIsNotNone(result)
                    # write_bookmark should be called
                    mock_write.assert_called_once()

    def test_update_start_date_bookmark_member_count(self):
        """Test update_start_date_bookmark with member_count type"""
        with patch('tap_sendgrid.context.bks_.get_bookmark', return_value=None):
            with patch('tap_sendgrid.context.bks_.write_bookmark'):
                with patch('tap_sendgrid.context.singer.write_state'):
                    ctx = Context(self.config, self.state)
                    result = ctx.update_start_date_bookmark(['groups_members', 'member_count'])

                    # Should return empty list for member_count
                    self.assertEqual(result, [])

    def test_save_member_count_state(self):
        """Test save_member_count_state updates state correctly"""
        with patch('tap_sendgrid.context.bks_.get_bookmark', return_value=[]):
            with patch('tap_sendgrid.context.bks_.write_bookmark'):
                with patch('tap_sendgrid.context.singer.write_state'):
                    ctx = Context(self.config, self.state)

                    stream_mock = Mock()
                    stream_mock.bookmark = ['groups_members', 'member_count']

                    list_item = {'id': '123', 'member_count': 100}

                    # This should not raise an error
                    try:
                        ctx.save_member_count_state(list_item, stream_mock)
                    except Exception as e:
                        self.fail(f"save_member_count_state raised {e}")


class TestContextOffsetManagement(unittest.TestCase):
    """Test cases for offset management in Context"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = {
            "start_date": "2026-02-01",
            "api_key": "test-api-key"
        }
        self.state = {}

    def test_offset_not_initialized_in_state(self):
        """Test that offset is not automatically initialized in state"""
        ctx = Context(self.config, self.state)

        # State should remain empty - no offset or bookmarks initially
        self.assertEqual(ctx.state, {})

    def test_bookmark_used_instead_of_offset(self):
        """Test that bookmarks are used for state tracking instead of offsets"""
        with patch('tap_sendgrid.context.bks_.get_bookmark', return_value='2026-02-01'):
            with patch('tap_sendgrid.context.bks_.write_bookmark'):
                with patch('tap_sendgrid.context.singer.write_state'):
                    ctx = Context(self.config, self.state)

                    result = ctx.get_bookmark(['contacts', 'timestamp'])

                    # Should use bookmarks, not offsets
                    self.assertEqual(result, '2026-02-01')
                    # Verify no offset in state
                    self.assertNotIn('offset', ctx.state)

    def test_set_bookmark_updates_state_without_offset(self):
        """Test that set_bookmark updates state without creating offset key"""
        with patch('tap_sendgrid.context.bks_.write_bookmark'):
            with patch('tap_sendgrid.context.singer.write_state'):
                ctx = Context(self.config, self.state)

                ctx.set_bookmark(['contacts', 'timestamp'], '2026-02-05')

                # Verify offset is not in state
                self.assertNotIn('offset', ctx.state)


class TestContextCatalogManagement(unittest.TestCase):
    """Test cases for catalog management"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = {
            "start_date": "2026-02-01",
            "api_key": "test-api-key"
        }
        self.state = {}

    def test_catalog_setter_filters_selected_streams(self):
        """Test that catalog setter filters selected streams"""
        ctx = Context(self.config, self.state)

        # Create mock catalog and streams
        stream1 = Mock()
        stream1.tap_stream_id = 'global_suppressions'
        stream1.is_selected.return_value = True

        stream2 = Mock()
        stream2.tap_stream_id = 'campaigns'
        stream2.is_selected.return_value = False

        catalog = Mock()
        catalog.streams = [stream1, stream2]

        # Set the catalog
        ctx.catalog = catalog

        # Verify selected_catalog contains only selected streams
        self.assertEqual(len(ctx.selected_catalog), 1)
        self.assertEqual(ctx.selected_catalog[0].tap_stream_id, 'global_suppressions')

        # Verify selected_stream_ids
        self.assertIn('global_suppressions', ctx.selected_stream_ids)
        self.assertNotIn('campaigns', ctx.selected_stream_ids)
