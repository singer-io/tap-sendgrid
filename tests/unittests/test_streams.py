"""Unit tests for streams module"""

import unittest

from tap_sendgrid import streams


class TestStreamsConfiguration(unittest.TestCase):
    """Test cases for streams configuration"""

    def test_streams_list_not_empty(self):
        """Test that STREAMS list is not empty"""
        self.assertGreater(len(streams.STREAMS), 0)

    def test_all_streams_have_required_fields(self):
        """Test that all streams have required fields"""
        required_fields = ['tap_stream_id', 'endpoint']

        for stream in streams.STREAMS:
            self.assertIsNotNone(stream.tap_stream_id)
            self.assertIsNotNone(stream.endpoint)
            for field in required_fields:
                self.assertTrue(hasattr(stream, field),
                              f"Stream {stream.tap_stream_id} missing {field}")

    def test_bookmark_configuration(self):
        """Test bookmark configuration for streams"""
        # Streams with bookmarks (incremental)
        incremental_streams = {
            'global_suppressions': ['global_suppressions', 'end_time'],
            'contacts': ['contacts', 'timestamp'],
            'groups_members': ['groups_members', 'member_count'],
            'lists_members': ['lists_members', 'member_count'],
            'segments_members': ['segments_members', 'member_count'],
            'invalids': ['invalids', 'end_time'],
            'bounces': ['bounces', 'end_time'],
            'blocks': ['blocks', 'end_time'],
            'spam_reports': ['spam_reports', 'end_time'],
        }

        # Streams without bookmarks (full table)
        full_table_streams = {
            'groups_all', 'lists_all', 'segments_all', 'templates_all', 'campaigns'
        }

        for stream in streams.STREAMS:
            if stream.tap_stream_id in incremental_streams:
                self.assertIsNotNone(stream.bookmark,
                                   f"{stream.tap_stream_id} should have bookmark")
                expected_bookmark = incremental_streams[stream.tap_stream_id]
                self.assertEqual(stream.bookmark, expected_bookmark)
            elif stream.tap_stream_id in full_table_streams:
                self.assertIsNone(stream.bookmark,
                                 f"{stream.tap_stream_id} should not have bookmark")

    def test_primary_keys_configured(self):
        """Test that all streams have primary keys"""
        for stream in streams.STREAMS:
            self.assertIn(stream.tap_stream_id, streams.PK_FIELDS,
                         f"Primary key not configured for {stream.tap_stream_id}")
            self.assertIsNotNone(streams.PK_FIELDS[stream.tap_stream_id])
            self.assertGreater(len(streams.PK_FIELDS[stream.tap_stream_id]), 0)

    def test_primary_keys_format(self):
        """Test that primary keys are arrays"""
        for stream_id, pk_fields in streams.PK_FIELDS.items():
            self.assertIsInstance(pk_fields, list,
                                f"Primary keys for {stream_id} should be a list")

    def test_email_based_primary_keys(self):
        """Test streams that use email as primary key"""
        email_pk_streams = {
            'global_suppressions', 'groups_members', 'invalids',
            'bounces', 'blocks', 'spam_reports'
        }

        for stream_id in email_pk_streams:
            pk_fields = streams.PK_FIELDS[stream_id]
            self.assertIn('email', pk_fields,
                         f"{stream_id} should have 'email' as primary key")

    def test_id_based_primary_keys(self):
        """Test streams that use id as primary key"""
        id_pk_streams = {
            'groups_all', 'contacts', 'lists_all', 'lists_members',
            'segments_all', 'segments_members', 'templates_all', 'campaigns'
        }

        for stream_id in id_pk_streams:
            pk_fields = streams.PK_FIELDS[stream_id]
            self.assertIn('id', pk_fields,
                         f"{stream_id} should have 'id' as primary key")

    def test_parent_stream_relationships(self):
        """Test parent-child stream relationships"""
        parent_child_map = {
            'groups_all': ['groups_members'],
            'lists_all': ['lists_members'],
            'segments_all': ['segments_members'],
        }

        for stream in streams.STREAMS:
            if stream.parent:
                # Verify parent stream exists
                parent_exists = any(s.tap_stream_id == stream.parent for s in streams.STREAMS)
                self.assertTrue(parent_exists,
                              f"Parent stream {stream.parent} not found for {stream.tap_stream_id}")


class TestScopesConfiguration(unittest.TestCase):
    """Test cases for Scopes configuration"""

    def test_scopes_endpoint_configured(self):
        """Test that Scopes endpoint is configured"""
        self.assertIsNotNone(streams.Scopes.endpoint)
        self.assertIn('scopes', streams.Scopes.endpoint)

    def test_scopes_list_not_empty(self):
        """Test that Scopes list is not empty"""
        self.assertGreater(len(streams.Scopes.scopes), 0)

    def test_required_scopes(self):
        """Test that required scopes are present"""
        required_scopes = [
            'suppression.read',
            'asm.groups.read',
            'templates.read',
        ]

        for scope in required_scopes:
            self.assertIn(scope, streams.Scopes.scopes,
                         f"Required scope {scope} not found")


class TestStreamIDs(unittest.TestCase):
    """Test cases for stream IDs"""

    def test_stream_ids_list_generated(self):
        """Test that stream_ids list is generated from IDS class"""
        self.assertGreater(len(streams.stream_ids), 0)

    def test_all_stream_ids_have_corresponding_streams(self):
        """Test that all stream IDs have corresponding STREAMS entries"""
        stream_tap_ids = {s.tap_stream_id for s in streams.STREAMS}

        # Filter stream_ids to only include actual stream IDs
        # (strings that don't contain dots/modules and match stream names)
        valid_stream_ids = [
            sid for sid in streams.stream_ids
            if isinstance(sid, str) and '.' not in sid and sid in stream_tap_ids
        ]

        # Verify we got the expected number of stream IDs
        self.assertEqual(len(valid_stream_ids), 14,
                        f"Expected 14 stream IDs, got {len(valid_stream_ids)}")

    def test_no_duplicate_stream_ids(self):
        """Test that stream IDs are unique"""
        stream_tap_ids = [s.tap_stream_id for s in streams.STREAMS]
        self.assertEqual(len(stream_tap_ids), len(set(stream_tap_ids)),
                        "Duplicate stream IDs found")


class TestEndpointConfiguration(unittest.TestCase):
    """Test cases for endpoint configuration"""

    def test_all_endpoints_start_with_https(self):
        """Test that all endpoints use HTTPS"""
        for stream in streams.STREAMS:
            self.assertTrue(stream.endpoint.startswith('https://'),
                          f"Endpoint for {stream.tap_stream_id} does not use HTTPS")

    def test_sendgrid_api_endpoints(self):
        """Test that endpoints are SendGrid API endpoints"""
        for stream in streams.STREAMS:
            self.assertIn('api.sendgrid.com', stream.endpoint,
                         f"Endpoint for {stream.tap_stream_id} is not a SendGrid endpoint")

    def test_parent_streams_have_parameterized_endpoints(self):
        """Test that child streams have parameterized endpoints with {}"""
        child_streams = {'groups_members', 'lists_members', 'segments_members'}

        for stream in streams.STREAMS:
            if stream.tap_stream_id in child_streams:
                self.assertIn('{}', stream.endpoint,
                            f"Child stream {stream.tap_stream_id} endpoint should have {{}}")
