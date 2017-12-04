from datetime import date
import pendulum
import singer
from singer import bookmarks as bks_

from .utils import trim_members_all, clean_for_cache

class Context(object):
    """Represents a collection of global objects necessary for performing
    discovery or for running syncs. Notably, it contains
    - config  - The JSON structure from the config.json argument
    - state   - The mutable state dict that is shared among streams
    - endpoint  - The SendGrid endpoint
    - catalog - A singer.catalog.Catalog. Note this will be None during
                discovery.
    - cache   - A place for streams to store data so it can be shared between
                streams.
    """

    def __init__(self, config, state):
        self.config = config
        self.state = state
        self._catalog = None
        self.selected_stream_ids = None
        self.selected_catalog = None
        self.cache = {}
        self.now = pendulum.now()
        self.now_seconds = pendulum.now().int_timestamp

    @property
    def catalog(self):
        return self._catalog

    @catalog.setter
    def catalog(self, catalog):
        self._catalog = catalog
        self.selected_stream_ids = set(
            [s.tap_stream_id for s in catalog.streams
             if s.is_selected()]
        )
        self.selected_catalog = [s for s in catalog.streams
                                 if s.is_selected()]

    def get_bookmark(self, path):
        return bks_.get_bookmark(self.state, *path)

    def set_bookmark(self, path, val):
        if isinstance(val, date):
            val = val.isoformat()
        bks_.write_bookmark(self.state, path[0], path[1], val)
        self.write_state()

    def get_offset(self, path):
        off = bks_.get_offset(self.state, path[0])
        return (off or {}).get(path[1])

    def set_offset(self, path, val):
        bks_.set_offset(self.state, path[0], path[1], val)

    def clear_offsets(self, tap_stream_id):
        bks_.clear_offset(self.state, tap_stream_id)

    def update_start_date_bookmark(self, path):
        val = self.get_bookmark(path)
        if path[1] == 'member_count':
            if not val:
                val = []
                self.set_bookmark(path, val)
            return val
        else:
            if not val:
                val = self.config["start_date"]
                self.set_bookmark(path, val)
            return pendulum.parse(val)

    def save_member_count_state(self, s, stream):
        old_state = self.update_start_date_bookmark(stream.bookmark)
        new_state = [s] + [os for os in old_state if os['id'] != s['id']]

        self.set_bookmark(stream.bookmark, new_state)

    def write_state(self):
        singer.write_state(self.state)

    def now_date_str(self):
        return self.now.to_rfc3339_string()

    @staticmethod
    def ts_to_dt(ts):
        return pendulum.from_timestamp(ts).to_rfc3339_string()

    def update_cache(self, data, stream_id):
        self.cache.update({
            trim_members_all(stream_id):
                clean_for_cache(data, stream_id)
        })
