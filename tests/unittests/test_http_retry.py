"""Unit tests for HTTP retry/backoff logic"""
import unittest
from unittest.mock import Mock, patch
import requests

from tap_sendgrid.http import authed_get, SendGridRateLimitException, SendGridServerException


class TestHttpRetry(unittest.TestCase):
    """Test cases for HTTP retry and backoff logic"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = {"api_key": "test-key"}

    @patch('tap_sendgrid.http.session.request')
    def test_successful_request(self, mock_request):
        """Test that successful requests work normally"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'result': []}
        mock_request.return_value = mock_response

        result = authed_get('test_stream', 'https://api.sendgrid.com/v3/test', self.config)
        
        self.assertEqual(result.status_code, 200)
        mock_request.assert_called_once()

    @patch('tap_sendgrid.http.session.request')
    def test_rate_limit_retry(self, mock_request):
        """Test that 429 rate limits trigger retries"""
        # First call returns 429, second call succeeds
        rate_limit_response = Mock()
        rate_limit_response.status_code = 429
        
        success_response = Mock()
        success_response.status_code = 200
        success_response.json.return_value = {'result': []}
        
        mock_request.side_effect = [rate_limit_response, success_response]

        result = authed_get('test_stream', 'https://api.sendgrid.com/v3/test', self.config)
        
        # Should have retried and succeeded
        self.assertEqual(result.status_code, 200)
        self.assertEqual(mock_request.call_count, 2)

    @patch('tap_sendgrid.http.session.request')
    def test_server_error_retry(self, mock_request):
        """Test that 5xx errors trigger retries"""
        # First call returns 503, second call succeeds
        server_error_response = Mock()
        server_error_response.status_code = 503
        
        success_response = Mock()
        success_response.status_code = 200
        success_response.json.return_value = {'result': []}
        
        mock_request.side_effect = [server_error_response, success_response]

        result = authed_get('test_stream', 'https://api.sendgrid.com/v3/test', self.config)
        
        # Should have retried and succeeded
        self.assertEqual(result.status_code, 200)
        self.assertEqual(mock_request.call_count, 2)

    @patch('tap_sendgrid.http.session.request')
    def test_max_retries_exhausted(self, mock_request):
        """Test that max retries are respected"""
        # Always return 429
        rate_limit_response = Mock()
        rate_limit_response.status_code = 429
        mock_request.return_value = rate_limit_response

        with self.assertRaises(SendGridRateLimitException):
            authed_get('test_stream', 'https://api.sendgrid.com/v3/test', self.config)
        
        # Should have tried max_tries times (5)
        self.assertEqual(mock_request.call_count, 5)

    @patch('tap_sendgrid.http.session.request')
    def test_non_retryable_error(self, mock_request):
        """Test that 4xx errors (except 429) don't retry"""
        error_response = Mock()
        error_response.status_code = 404
        error_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
        mock_request.return_value = error_response

        with self.assertRaises(requests.exceptions.HTTPError):
            authed_get('test_stream', 'https://api.sendgrid.com/v3/test', self.config)
        
        # Should have tried only once (not retryable)
        self.assertEqual(mock_request.call_count, 1)

    @patch('tap_sendgrid.http.session.request')
    def test_connection_error_retry(self, mock_request):
        """Test that connection errors trigger retries"""
        # First call raises connection error, second succeeds
        success_response = Mock()
        success_response.status_code = 200
        success_response.json.return_value = {'result': []}
        
        mock_request.side_effect = [
            requests.exceptions.ConnectionError(),
            success_response
        ]

        result = authed_get('test_stream', 'https://api.sendgrid.com/v3/test', self.config)
        
        # Should have retried and succeeded
        self.assertEqual(result.status_code, 200)
        self.assertEqual(mock_request.call_count, 2)

    @patch('tap_sendgrid.http.session.request')
    def test_timeout_retry(self, mock_request):
        """Test that timeouts trigger retries"""
        # First call times out, second succeeds
        success_response = Mock()
        success_response.status_code = 200
        success_response.json.return_value = {'result': []}
        
        mock_request.side_effect = [
            requests.exceptions.Timeout(),
            success_response
        ]

        result = authed_get('test_stream', 'https://api.sendgrid.com/v3/test', self.config)
        
        # Should have retried and succeeded
        self.assertEqual(result.status_code, 200)
        self.assertEqual(mock_request.call_count, 2)


if __name__ == '__main__':
    unittest.main()
