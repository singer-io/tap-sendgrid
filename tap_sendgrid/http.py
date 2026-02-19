import requests
import singer
from singer import metrics
import backoff

LOGGER = singer.get_logger()

session = requests.Session()


class SendGridRateLimitException(Exception):
    pass


class SendGridServerException(Exception):
    pass


def is_retryable(e):
    """Determine if exception is retryable"""
    if isinstance(e, SendGridRateLimitException):
        return True
    if isinstance(e, SendGridServerException):
        return True
    if isinstance(e, requests.exceptions.ConnectionError):
        return True
    if isinstance(e, requests.exceptions.Timeout):
        return True
    return False


@backoff.on_exception(
    backoff.expo,
    (SendGridRateLimitException, SendGridServerException, requests.exceptions.RequestException),
    max_tries=5,
    giveup=lambda e: not is_retryable(e),
    factor=2
)
def authed_get(tap_stream_id, url, config, params=None):
    headers = {"Authorization": "Bearer %s" % config['api_key']}
    with metrics.http_request_timer(tap_stream_id) as timer:
        resp = session.request(method='get', url=url, params=params, headers=headers)
        timer.tags[metrics.Tag.http_status_code] = resp.status_code

        # Handle rate limiting
        if resp.status_code == 429:
            LOGGER.warning("Rate limit hit for %s, retrying..." % tap_stream_id)
            raise SendGridRateLimitException("Rate limit exceeded")

        # Handle server errors
        if resp.status_code >= 500:
            LOGGER.warning("Server error %s for %s, retrying..." % (resp.status_code, tap_stream_id))
            raise SendGridServerException("Server error: %s" % resp.status_code)

        # Raise for other bad status codes
        resp.raise_for_status()

        return resp


def end_of_records_check(r):
    empty_message = "No more pages"
    if r.status_code == 404 and r.json().get(
            'errors', [{}])[0].get('message') == empty_message:
        return True
    if r.json().get('recipient_count') == 0:
        return True
    else:
        return False
