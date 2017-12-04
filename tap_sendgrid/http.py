import requests
from singer import metrics

session = requests.Session()


def authed_get(tap_stream_id, url, config, params=None):
    headers = {"Authorization": "Bearer %s" % config['api_key']}
    with metrics.http_request_timer(tap_stream_id) as timer:
        resp = session.request(method='get', url=url, params=params, headers=headers)
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
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
