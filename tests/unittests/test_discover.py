from tap_sendgrid.discover import discover


def test_discover_has_streams():
    catalog = discover()
    stream_names = {stream.stream for stream in catalog.streams}
    assert "blocks" in stream_names
    assert "templates" in stream_names
    assert "suppression_group_members" in stream_names
