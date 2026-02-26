# Changelog

## 0.2.0
- Fixed interrupted sync resume order: streams now resume in circular STREAMS-dict
  order from `currently_syncing`, matching the original sync sequence regardless
  of which bookmarks are present in the incoming state.
- Fixed `write_schema` child-stream handling: children are now only added to
  `parent.child_to_sync` when the child is selected in the catalog, preventing
  unnecessary API calls when neither parent nor child is selected.  Added guard
  against `None` catalog entries for child streams.
- Fixed `FullTableStream.default_params`: no longer reads `page_size` from
  config (SendGrid marketing endpoints cap at 100; the fixed class-level
  `page_size = 50` is used instead, preventing HTTP 400 on user-supplied
  large values).
- Fixed stale `MISSING_FIELDS` in `test_all_fields.py`: removed `contact_count`
  annotation for `segments` (field was renamed `contacts_count` in the schema
  and is correctly returned by the API).
- Fixed CircleCI: added `--junitxml` flag so test results are correctly stored.
- Updated README to document `page_size` behaviour accurately.
- Applied KISS/DRY pass across `streams/`: extracted shared base classes,
  removed redundant class-level attribute declarations, dropped overrides
  identical to their parent.
- Updated all schemas to match real API field names and types (verified against
  live SendGrid API responses).

## 0.1.0
- Initial Singer-compatible `tap-sendgrid` implementation.
- Added discovery, sync, state/bookmarks, incremental and full-table streams.
- Added retry/backoff, `Retry-After` handling, and parent-child orchestration.
- Added tap-tester style integration test suite scaffold and unit tests.
- Added CI and lint/test documentation.
