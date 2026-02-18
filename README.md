# tap-sendgrid

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from SendGrid's [REST API v3](https://docs.sendgrid.com/api-reference)
- Extracts the following resources from SendGrid
  - [Global Suppressions](https://docs.sendgrid.com/api-reference/suppressions-global-suppressions/retrieve-all-global-suppressions) - Email addresses globally unsubscribed from all emails
  - [Suppression Groups](https://docs.sendgrid.com/api-reference/suppressions-unsubscribe-groups/retrieve-all-suppression-groups-associated-with-the-user) - Unsubscribe groups
  - [Suppression Group Members](https://docs.sendgrid.com/api-reference/suppressions-suppressions/retrieve-all-suppressions-for-a-suppression-group) - Email addresses in suppression groups
  - [Lists](https://docs.sendgrid.com/api-reference/lists/get-all-lists) - Marketing contact lists
  - [Segments](https://docs.sendgrid.com/api-reference/segmenting-contacts-v2/get-list-of-segments) - Dynamic contact segments (v2.0)
  - [Single Sends](https://docs.sendgrid.com/api-reference/single-sends/get-all-single-sends) - One-time email campaigns
  - [Templates](https://docs.sendgrid.com/api-reference/transactional-templates/retrieve-paged-transactional-templates) - Transactional email templates
  - [Invalid Emails](https://docs.sendgrid.com/api-reference/invalid-e-mails-api/retrieve-all-invalid-emails) - Invalid email addresses
  - [Bounces](https://docs.sendgrid.com/api-reference/bounces-api/retrieve-all-bounces) - Bounced email addresses
  - [Blocks](https://docs.sendgrid.com/api-reference/blocks-api/retrieve-all-blocks) - Blocked email addresses
  - [Spam Reports](https://docs.sendgrid.com/api-reference/spam-reports-api/retrieve-all-spam-reports) - Spam report email addresses
- Outputs the schema for each resource
- Pulls data, incrementally based on input state where possible

## Configuration

This tap requires a `config.json` which specifies details start date and API key.

## Run Discovery

To run discovery mode, execute the tap with the config file.

```
> tap-sendgrid --config config.json --discovery > properties.json
```

## Sync Data

To sync data, select fields in the `properties.json` output and run the tap.

```
> tap-sendgrid --config config.json --properties properties.json [--state state.json]
```

Copyright &copy; 2018 Stitch
