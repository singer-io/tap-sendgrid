"""Unit tests for state management"""

import unittest
from unittest.mock import patch

from tap_sendgrid.context import Context


class TestStateManagement(unittest.TestCase):
    """Test cases for state management"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = {
            "start_date": "2026-02-01",
            "api_key": "test-api-key"
        }
        self.state = {}

    def test_state_no_offset_key_on_init(self):
        """Test that state does not have offset key on initialization"""
        ctx = Context(self.config, self.state)

        # Offset should not be in state
        self.assertNotIn('offset', ctx.state)

    def test_state_no_offset_after_bookmark_operations(self):
        """Test that offset is not added after bookmark operations"""
        with patch('tap_sendgrid.context.bks_.get_bookmark', return_value=None):
            with patch('tap_sendgrid.context.bks_.write_bookmark') as mock_write:
                with patch('tap_sendgrid.context.singer.write_state'):
                    ctx = Context(self.config, self.state)
                    ctx.set_bookmark(['test_stream', 'test_key'], '2026-02-01')

                    # Offset should still not be in state
                    self.assertNotIn('offset', ctx.state)

    def test_state_write_called_on_bookmark_set(self):
        """Test that write_state is called when bookmark is set"""
        with patch('tap_sendgrid.context.bks_.get_bookmark', return_value=None):
            with patch('tap_sendgrid.context.bks_.write_bookmark'):
                with patch('tap_sendgrid.context.singer.write_state') as mock_write_state:
                    ctx = Context(self.config, self.state)
                    ctx.set_bookmark(['test_stream', 'test_key'], '2026-02-01')

                    # write_state should be called
                    mock_write_state.assert_called()

    def test_offset_not_used_in_any_operation(self):
        """Test that offset is not used at all in state management"""
        with patch('tap_sendgrid.context.bks_.get_bookmark', return_value=None):
            with patch('tap_sendgrid.context.bks_.write_bookmark'):
                with patch('tap_sendgrid.context.singer.write_state'):
                    ctx = Context(self.config, self.state)

                    # Perform bookmark operations which are the only state ops
                    ctx.update_start_date_bookmark(['test_stream', 'test_key'])

                    # offset should not be in state
                    self.assertNotIn('offset', ctx.state)

    def test_state_structure_clean(self):
        """Test that state maintains clean structure without offset"""
        with patch('tap_sendgrid.context.bks_.get_bookmark', return_value='2026-02-01'):
            with patch('tap_sendgrid.context.bks_.write_bookmark'):
                with patch('tap_sendgrid.context.singer.write_state'):
                    ctx = Context(self.config, self.state)

                    # State should remain clean
                    self.assertEqual(ctx.state, {})


class TestBookmarkTypes(unittest.TestCase):
    """Test cases for different bookmark types"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = {
            "start_date": "2026-02-01",
            "api_key": "test-api-key"
        }

    def test_end_time_bookmark_handling(self):
        """Test end_time bookmark type"""
        state = {}
        with patch('tap_sendgrid.context.bks_.get_bookmark', return_value='2026-02-01T10:00:00Z'):
            ctx = Context(self.config, state)
            result = ctx.update_start_date_bookmark(['global_suppressions', 'end_time'])

            self.assertIsNotNone(result)

    def test_timestamp_bookmark_handling(self):
        """Test timestamp bookmark type"""
        state = {}
        with patch('tap_sendgrid.context.bks_.get_bookmark', return_value='2026-02-01T10:00:00Z'):
            ctx = Context(self.config, state)
            result = ctx.update_start_date_bookmark(['contacts', 'timestamp'])

            self.assertIsNotNone(result)

    def test_member_count_bookmark_handling(self):
        """Test member_count bookmark type"""
        state = {}
        with patch('tap_sendgrid.context.bks_.get_bookmark', return_value=None):
            with patch('tap_sendgrid.context.bks_.write_bookmark'):
                with patch('tap_sendgrid.context.singer.write_state'):
                    ctx = Context(self.config, state)
                    result = ctx.update_start_date_bookmark(['groups_members', 'member_count'])

                    # member_count should return a list
                    self.assertIsInstance(result, list)
