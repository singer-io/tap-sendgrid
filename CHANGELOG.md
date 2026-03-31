# Changelog

## 2.0.0
* Full rewrite targeting the SendGrid v3 Marketing API. Re-discovery and re-configuration required for existing connections.
* **Breaking:** Streams removed (deprecated v2 API endpoints) — `contacts`, `lists_members`, `segments_members`, `campaigns`.
* **Breaking:** `lists` and `segments` moved to v3 Marketing API endpoints (`/v3/marketing/lists`, `/v3/marketing/segments`).
* New streams: `single_sends`, `single_send_stats`, `stats_automations`, `senders`, `marketing_contacts_count`, `marketing_field_definitions`.
* Updated dependencies: `singer-python` 5.13.2 → 6.1.1, `backoff==2.2.1` added, `pendulum` and `pytz` removed.

## 1.0.5
* Dependency upgrades [#15](https://github.com/singer-io/tap-sendgrid/pull/15)

## 1.0.4
  * Add pytz to install_requires [#13](https://github.com/singer-io/tap-sendgrid/pull/13)

## 1.0.3
  * Reverts #3

## 1.0.2
  * Updates permissions used for reading campaigns [#3](https://github.com/singer-io/tap-sendgrid/pull/3)

## 1.0.1
  * Update version of `requests` to `2.20.0` in response to CVE 2018-18074

## 0.1.2
  * Fix filtering code to include automatic fields [#2](https://github.com/singer-io/tap-sendgrid/pull/2)

## 0.1.1
  * Setting key properties to automatic [#1](https://github.com/singer-io/tap-sendgrid/pull/1)
