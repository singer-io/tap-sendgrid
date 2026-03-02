# tap-sendgrid

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This tap:

- Pulls raw data from the [SendGrid v3 API](https://docs.sendgrid.com/api-reference)
- Extracts the following resources:
  - [Blocks](https://docs.sendgrid.com/api-reference/blocks-api/retrieve-all-blocks)
  - [Bounces](https://docs.sendgrid.com/api-reference/bounces-api/retrieve-all-bounces)
  - [Spam Reports](https://docs.sendgrid.com/api-reference/spam-reports-api/retrieve-all-spam-reports)
  - [Invalid Emails](https://docs.sendgrid.com/api-reference/invalid-e-mails-api/retrieve-all-invalid-emails)
  - [Global Suppressions](https://docs.sendgrid.com/api-reference/suppressions-global-suppressions/retrieve-all-global-suppressions)
  - [Lists](https://docs.sendgrid.com/api-reference/lists/get-all-lists)
  - [Segments](https://docs.sendgrid.com/api-reference/segmenting-contacts-v2/get-list-of-segments)
  - [Single Sends](https://docs.sendgrid.com/api-reference/single-sends/get-all-single-sends)
  - [Single Send Stats](https://docs.sendgrid.com/api-reference/single-send-stats/get-all-single-sends-stats)
  - [Stats Automations](https://docs.sendgrid.com/api-reference/marketing-email-stats/get-all-automation-stats)
  - [Senders](https://docs.sendgrid.com/api-reference/sender-identities-api/get-all-sender-identities)
  - [Marketing Contacts Count](https://docs.sendgrid.com/api-reference/contacts/get-contact-count)
  - [Marketing Field Definitions](https://docs.sendgrid.com/api-reference/custom-fields/get-all-field-definitions)
  - [Suppression Groups](https://docs.sendgrid.com/api-reference/suppressions-unsubscribe-groups/retrieve-all-suppression-groups-associated-with-the-user)
  - [Suppression Group Members](https://docs.sendgrid.com/api-reference/suppressions-suppressions/retrieve-all-suppressions-for-a-suppression-group)
  - [Templates](https://docs.sendgrid.com/api-reference/transactional-templates/retrieve-paged-transactional-templates)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

---

## Streams

[blocks](https://docs.sendgrid.com/api-reference/blocks-api/retrieve-all-blocks)

- Primary keys: `['email']`
- Replication strategy: INCREMENTAL (bookmark: `created`)
- Pagination: offset (`limit` / `offset`)

[bounces](https://docs.sendgrid.com/api-reference/bounces-api/retrieve-all-bounces)

- Primary keys: `['email']`
- Replication strategy: INCREMENTAL (bookmark: `created`)
- Pagination: offset (`limit` / `offset`)

[spam_reports](https://docs.sendgrid.com/api-reference/spam-reports-api/retrieve-all-spam-reports)

- Primary keys: `['email']`
- Replication strategy: INCREMENTAL (bookmark: `created`)
- Pagination: offset (`limit` / `offset`)

[invalid_emails](https://docs.sendgrid.com/api-reference/invalid-e-mails-api/retrieve-all-invalid-emails)

- Primary keys: `['email']`
- Replication strategy: INCREMENTAL (bookmark: `created`)
- Pagination: offset (`limit` / `offset`)

[global_suppressions](https://docs.sendgrid.com/api-reference/suppressions-global-suppressions/retrieve-all-global-suppressions)

- Primary keys: `['email']`
- Replication strategy: INCREMENTAL (bookmark: `created`)
- Pagination: offset (`limit` / `offset`)

[lists](https://docs.sendgrid.com/api-reference/lists/get-all-lists)

- Primary keys: `['id']`
- Replication strategy: FULL_TABLE
- Pagination: cursor (`_metadata.next`)

[segments](https://docs.sendgrid.com/api-reference/segmenting-contacts-v2/get-list-of-segments)

- Primary keys: `['id']`
- Replication strategy: FULL_TABLE
- Pagination: cursor (`_metadata.next`)

[single_sends](https://docs.sendgrid.com/api-reference/single-sends/get-all-single-sends)

- Primary keys: `['id']`
- Replication strategy: FULL_TABLE
- Pagination: cursor (`_metadata.next`)

[single_send_stats](https://docs.sendgrid.com/api-reference/single-send-stats/get-all-single-sends-stats)

- Primary keys: `['id']`
- Replication strategy: FULL_TABLE
- Pagination: cursor (`_metadata.next`)

[stats_automations](https://docs.sendgrid.com/api-reference/marketing-email-stats/get-all-automation-stats)

- Primary keys: `['id']`
- Replication strategy: FULL_TABLE
- Pagination: cursor (`_metadata.next`)

[senders](https://docs.sendgrid.com/api-reference/sender-identities-api/get-all-sender-identities)

- Primary keys: `['id']`
- Replication strategy: FULL_TABLE
- Pagination: none (single list response)

[marketing_contacts_count](https://docs.sendgrid.com/api-reference/contacts/get-contact-count)

- Primary keys: none
- Replication strategy: FULL_TABLE
- Pagination: none (single aggregate response)

[marketing_field_definitions](https://docs.sendgrid.com/api-reference/custom-fields/get-all-field-definitions)

- Primary keys: `['id']`
- Replication strategy: FULL_TABLE
- Pagination: none (merges `reserved_fields` and `custom_fields` arrays)

[suppression_groups](https://docs.sendgrid.com/api-reference/suppressions-unsubscribe-groups/retrieve-all-suppression-groups-associated-with-the-user)

- Primary keys: `['id']`
- Replication strategy: FULL_TABLE
- Pagination: none (single list response)

[suppression_group_members](https://docs.sendgrid.com/api-reference/suppressions-suppressions/retrieve-all-suppressions-for-a-suppression-group)

- Primary keys: `['group_id', 'recipient_email']`
- Replication strategy: FULL_TABLE
- Pagination: none (single list response per parent group)
- Parent stream: `suppression_groups`

[templates](https://docs.sendgrid.com/api-reference/transactional-templates/retrieve-paged-transactional-templates)

- Primary keys: `['id']`
- Replication strategy: FULL_TABLE
- Pagination: cursor (`_metadata.next`)

---

## Authentication

Generate an API key at **Settings → API Keys** in the [SendGrid dashboard](https://app.sendgrid.com/settings/api_keys).

Required permission scopes:

| Scope group | Streams |
|---|---|
| `Suppressions → Read` | `blocks`, `bounces`, `spam_reports`, `invalid_emails`, `global_suppressions` |
| `ASM → Groups → Read` | `suppression_groups`, `suppression_group_members` |
| `Templates → Read` | `templates` |
| `Marketing → Read` | `lists`, `segments`, `single_sends`, `single_send_stats`, `stats_automations`, `senders`, `marketing_contacts_count`, `marketing_field_definitions` |

---

## Quick Start

1. **Install**

   Clone this repository, then install using `setup.py`. A virtualenv is recommended:

   ```bash
   virtualenv -p python3 venv
   source venv/bin/activate
   python setup.py install
   ```

   Or with pip:

   ```bash
   pip install -e .
   ```

2. **Dependent libraries**

   The following libraries are installed automatically:

   ```bash
   pip install singer-python
   pip install target-stitch
   pip install target-json
   ```

   - [singer-tools](https://github.com/singer-io/singer-tools)
   - [target-stitch](https://github.com/singer-io/target-stitch)

3. **Create your tap's `config.json` file**

   The tap config file for this tap should include these entries:

   - `api_key` (string, required): SendGrid API key (starts with `SG.`)
   - `start_date` (string, required): RFC 3339 start date for incremental streams (e.g. `2024-01-01T00:00:00Z`)
   - `request_timeout` (integer, optional): HTTP request timeout in seconds. Default is `300`.
   - `page_size` (integer, optional): Records per page for incremental suppression streams (`blocks`, `bounces`, `spam_reports`, `invalid_emails`, `global_suppressions`). Default is `500`. Full-table marketing streams use a fixed page size of `50` regardless of this setting (SendGrid caps those endpoints at 100).
   - `lookback_window_days` (integer, optional): Days to subtract from the bookmark on each incremental run to catch late-arriving records. Default is `0`.

   ```json
   {
     "api_key": "SG.xxxxxx",
     "start_date": "2024-01-01T00:00:00Z",
     "request_timeout": 300,
     "page_size": 500,
     "lookback_window_days": 1
   }
   ```

   Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

   ```json
   {
     "currently_syncing": "blocks",
     "bookmarks": {
       "blocks":              { "created": "2024-01-01T00:00:00+00:00" },
       "bounces":             { "created": "2024-01-01T00:00:00+00:00" },
       "spam_reports":        { "created": "2024-01-01T00:00:00+00:00" },
       "invalid_emails":      { "created": "2024-01-01T00:00:00+00:00" },
       "global_suppressions": { "created": "2024-01-01T00:00:00+00:00" }
     }
   }
   ```

   The `created` bookmark stores an **ISO 8601 datetime string** (UTC). Incremental streams resume from the last saved bookmark value.

4. **Run the Tap in Discovery Mode**

   This creates a `catalog.json` for selecting objects/fields to integrate:

   ```bash
   tap-sendgrid --config config.json --discover > catalog.json
   ```

   See the Singer docs on discovery mode [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. **Run the Tap in Sync Mode** (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

   For Sync mode:

   ```bash
   tap-sendgrid --config config.json --catalog catalog.json > state.json
   tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
   ```

   To load to JSON files to verify outputs:

   ```bash
   tap-sendgrid --config config.json --catalog catalog.json | target-json > state.json
   tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
   ```

   To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:

   ```bash
   tap-sendgrid --config config.json --catalog catalog.json \
     | target-stitch --config target_config.json --dry-run > state.json
   tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
   ```

6. **Test the Tap**

   While developing the tap, the following utilities were run in accordance with Singer.io best practices:

   Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#code-quality):

   ```bash
   pylint tap_sendgrid
   ```

   Pylint test resulted in the following score:

   ```
   Your code has been rated at 10.00/10
   ```

   To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:

   ```bash
   tap-sendgrid --config config.json --catalog catalog.json | singer-check-tap > state.json
   tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
   ```

   #### Unit Tests

   Unit tests may be run with the following:

   ```bash
   python -m pytest tests/unittests --verbose
   ```

   Note, you may need to install test dependencies:

   ```bash
   pip install -e '.[dev]'
   ```

---

Copyright &copy; 2018 Stitch
