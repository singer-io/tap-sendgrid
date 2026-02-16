"""
Integration test suite for tap-sendgrid offset key validation
This test ensures that the offset key is NOT used in tap-sendgrid state management
"""

import unittest
from unittest.mock import Mock, patch

from tap_sendgrid.context import Context
from tap_sendgrid.syncs import Syncer


class TestOffsetKeyValidation(unittest.TestCase):
    """
    Comprehensive tests to validate that offset key is NOT present
    in tap-sendgrid state management
    """

    def setUp(self):
        """Set up test fixtures"""
        self.config = {
            "start_date": "2026-02-01",
            "api_key": "test-api-key"
        }
        self.state = {}

    def test_offset_completely_absent_from_state_initialization(self):
        """
        CRITICAL TEST: Verify offset key is completely absent
        after Context initialization
        """
        ctx = Context(self.config, self.state)

        # Assert offset is NOT in state
        self.assertNotIn('offset', ctx.state,
                        "FAIL: 'offset' key should NOT be in state")

        # Assert state is completely empty
        self.assertEqual(ctx.state, {},
                        "FAIL: state should be empty, no offset key")

    def test_offset_not_used_in_bookmark_operations(self):
        """
        CRITICAL TEST: Verify offset is not created during bookmark operations
        """
        with patch('tap_sendgrid.context.bks_.get_bookmark', return_value=None):
            with patch('tap_sendgrid.context.bks_.write_bookmark'):
                with patch('tap_sendgrid.context.singer.write_state'):
                    ctx = Context(self.config, self.state)

                    ctx.set_bookmark(['stream_id', 'bookmark_key'], '2026-02-01')

                    # Assert offset NOT created
                    self.assertNotIn('offset', ctx.state,
                                    "FAIL: offset should not be created during bookmark ops")

    def test_state_remains_clean_throughout_operations(self):
        """
        CRITICAL TEST: Verify state remains clean (no offset) through all operations
        """
        with patch('tap_sendgrid.context.bks_.get_bookmark', return_value=None):
            with patch('tap_sendgrid.context.bks_.write_bookmark'):
                with patch('tap_sendgrid.context.singer.write_state'):
                    ctx = Context(self.config, self.state)

                    # Perform multiple bookmark operations
                    ctx.set_bookmark(['stream1', 'key1'], 'value1')
                    ctx.set_bookmark(['stream2', 'key2'], 'value2')
                    ctx.update_start_date_bookmark(['stream3', 'key3'])

                    # Assert state remains clean
                    self.assertNotIn('offset', ctx.state,
                                    "FAIL: offset found in state after multiple operations")

    def test_syncer_does_not_use_offset(self):
        """
        CRITICAL TEST: Verify Syncer class doesn't initialize or use offset
        """
        ctx = Mock()
        ctx.state = {}

        syncer = Syncer(ctx)

        # Assert offset NOT in context state
        self.assertNotIn('offset', ctx.state,
                        "FAIL: offset should not appear in context state")

    def test_offset_key_validation_across_all_streams(self):
        """
        CRITICAL TEST: Verify offset handling is consistent across all stream types
        """
        from tap_sendgrid import streams

        with patch('tap_sendgrid.context.bks_.get_bookmark', return_value=None):
            with patch('tap_sendgrid.context.bks_.write_bookmark'):
                with patch('tap_sendgrid.context.singer.write_state'):
                    ctx = Context(self.config, self.state)

                    for stream in streams.STREAMS:
                        if stream.bookmark:
                            # Incremental stream
                            ctx.update_start_date_bookmark(stream.bookmark)
                            self.assertNotIn('offset', ctx.state,
                                            f"FAIL: offset created for {stream.tap_stream_id}")

    def test_bookmarks_used_instead_of_offsets(self):
        """
        TEST: Verify bookmarks are used, NOT offsets
        """
        with patch('tap_sendgrid.context.bks_.get_bookmark', return_value='2026-02-01'):
            with patch('tap_sendgrid.context.bks_.write_bookmark'):
                with patch('tap_sendgrid.context.singer.write_state'):
                    ctx = Context(self.config, self.state)

                    # Get bookmark
                    result = ctx.get_bookmark(['stream_id', 'bookmark_key'])
                    # Should have used bookmarks
                    self.assertEqual(result, '2026-02-01')
                    # Assert offset NOT in state
                    self.assertNotIn('offset', ctx.state)
