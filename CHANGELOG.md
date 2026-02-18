# Changelog

## 1.2.0
* **BREAKING CHANGES**: Migrated from deprecated Legacy Marketing Campaigns API to SendGrid Marketing API v3
  * Removed deprecated scope `marketing_campaigns.read` which is no longer available in SendGrid API ([SendGrid Migration Guide](https://docs.sendgrid.com/for-developers/sending-email/migrating-from-legacy-marketing-campaigns))
  * Removed 3 deprecated streams that relied on Legacy Marketing Campaigns API:
    - `contacts` - Previously used `/v3/contactdb/recipients/search` endpoint (deprecated)
    - `lists_members` - Previously used `/v3/contactdb/lists/{}/recipients` endpoint (deprecated)
    - `segments_members` - Previously used `/v3/contactdb/segments/{}/recipients` endpoint (deprecated)
  
  * **API Endpoint Migrations** (References: [SendGrid API v3 Docs](https://docs.sendgrid.com/api-reference)):
    - `lists_all`: Migrated from `/v3/contactdb/lists` to `/v3/marketing/lists` ([Lists API](https://docs.sendgrid.com/api-reference/lists/get-all-lists))
    - `segments_all`: Migrated from `/v3/contactdb/segments` to `/v3/marketing/segments/2.0` ([Segments v2 API](https://docs.sendgrid.com/api-reference/segmenting-contacts-v2/get-list-of-segments))
    - `campaigns`: Migrated from `/v3/campaigns` to `/v3/marketing/singlesends` ([Single Sends API](https://docs.sendgrid.com/api-reference/single-sends/get-all-single-sends))
  
  * **Schema Updates** to match new API response structures:
    - `lists_all`: Changed `id` type from integer to string, renamed `recipient_count` to `contact_count`, added `_metadata` object (per Marketing Lists API v3)
    - `segments_all`: Changed `id` type from integer to string, renamed `recipient_count` to `contacts_count`, added v2.0 fields: `created_at`, `updated_at`, `parent_list_ids`, `query_version`, `status` (per Segments v2 API)
    - `campaigns`: Complete schema restructure for Single Sends format - changed `id` to string, removed legacy fields (`title`, `subject`, `sender_id`, `list_ids`, `segment_ids`, `suppression_group_id`, `custom_unsubscribe_url`, `ip_pool`, `html_content`, `plain_content`), added Single Sends fields: `name`, `status`, `send_at`, `created_at`, `updated_at`, `categories` (per Single Sends API)
  
  * **Code Improvements**:
    - Fixed catalog handling to support both `--catalog` flag (new Singer spec) and `--properties` flag (legacy) for backward compatibility
    - Fixed field selection logic to properly read `selected` metadata from catalog instead of attempting to access non-existent property attributes
    - Updated `get_results_from_payload()` utility to handle new Marketing API response format with `result` and `_metadata` keys
    - Updated `clean_for_cache()` to use `contact_count` field instead of deprecated `recipient_count`
    - Added None check in `trimmed_records()` to prevent crashes when API returns no data
    - Removed segments from member count tracking as this is no longer supported in v2.0 API
  
  * **Remaining Streams** (11 total, all verified against current SendGrid API documentation):
    - `global_suppressions` - [GET /v3/suppression/unsubscribes](https://docs.sendgrid.com/api-reference/suppressions-global-suppressions/retrieve-all-global-suppressions)
    - `groups_all` - [GET /v3/asm/groups](https://docs.sendgrid.com/api-reference/suppressions-unsubscribe-groups/retrieve-all-suppression-groups-associated-with-the-user)
    - `groups_members` - [GET /v3/asm/groups/{}/suppressions](https://docs.sendgrid.com/api-reference/suppressions-suppressions/retrieve-all-suppressions-for-a-suppression-group)
    - `lists_all` - [GET /v3/marketing/lists](https://docs.sendgrid.com/api-reference/lists/get-all-lists)
    - `segments_all` - [GET /v3/marketing/segments/2.0](https://docs.sendgrid.com/api-reference/segmenting-contacts-v2/get-list-of-segments)
    - `templates_all` - [GET /v3/templates](https://docs.sendgrid.com/api-reference/transactional-templates/retrieve-paged-transactional-templates)
    - `invalids` - [GET /v3/suppression/invalid_emails](https://docs.sendgrid.com/api-reference/invalid-e-mails-api/retrieve-all-invalid-emails)
    - `bounces` - [GET /v3/suppression/bounces](https://docs.sendgrid.com/api-reference/bounces-api/retrieve-all-bounces)
    - `blocks` - [GET /v3/suppression/blocks](https://docs.sendgrid.com/api-reference/blocks-api/retrieve-all-blocks)
    - `spam_reports` - [GET /v3/suppression/spam_reports](https://docs.sendgrid.com/api-reference/spam-reports-api/retrieve-all-spam-reports)
    - `campaigns` - [GET /v3/marketing/singlesends](https://docs.sendgrid.com/api-reference/single-sends/get-all-single-sends)

## 1.1.0
* Metadata Updates [#18](https://github.com/singer-io/tap-sendgrid/pull/18)

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
