"""Unit tests for discovery functionality"""

import unittest
from unittest.mock import Mock, patch

from tap_sendgrid import streams


class TestStreamConfiguration(unittest.TestCase):
    """Test cases for stream configuration"""

    def test_streams_have_tap_stream_id(self):
        """Test that all streams have tap_stream_id"""
        for stream in streams.STREAMS:
            self.assertTrue(hasattr(stream, 'tap_stream_id'))
            self.assertIsNotNone(stream.tap_stream_id)

    def test_streams_have_endpoint(self):
        """Test that all streams have endpoint"""
        for stream in streams.STREAMS:
            self.assertTrue(hasattr(stream, 'endpoint'))
            self.assertIsNotNone(stream.endpoint)

    def test_streams_list_not_empty(self):
        """Test that STREAMS list contains streams"""
        self.assertGreater(len(streams.STREAMS), 0)
        self.assertEqual(len(streams.STREAMS), 14)  # tap-sendgrid has 14 streams


class TestBookmarkConfiguration(unittest.TestCase):
    """Test cases for bookmark configuration"""

    def test_bookmarks_class_exists(self):
        """Test that BOOKMARKS class is defined"""
        self.assertTrue(hasattr(streams, 'BOOKMARKS'))

    def test_pk_fields_defined(self):
        """Test that PK_FIELDS is defined"""
        self.assertTrue(hasattr(streams, 'PK_FIELDS'))

    def test_incremental_streams_have_bookmarks(self):
        """Test that incremental streams have bookmarks defined"""
        incremental_streams = [s for s in streams.STREAMS if s.bookmark]
        # tap-sendgrid has 9 incremental streams
        self.assertEqual(len(incremental_streams), 9)

    def test_full_table_streams_no_bookmarks(self):
        """Test that full-table streams don't have bookmarks"""
        full_table_streams = [s for s in streams.STREAMS if not s.bookmark]
        # tap-sendgrid has 5 full-table streams
        self.assertEqual(len(full_table_streams), 5)
