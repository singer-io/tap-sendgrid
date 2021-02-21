# tap-sendgrid

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from SendGrid's [REST API](https://sendgrid.com/docs/API_Reference/api_v3.html)
- Extracts the following resources from SendGrid
  - [Contacts](https://sendgrid.com/docs/API_Reference/Web_API_v3/Marketing_Campaigns/contactdb.html#Get-Recipients-Matching-Search-Criteria-GET)
  - [Global Suppressions](https://sendgrid.com/docs/API_Reference/Web_API_v3/Suppression_Management/global_suppressions.html#-Global-Unsubscribes)
  - [Suppression Groups](https://sendgrid.com/docs/API_Reference/Web_API_v3/Suppression_Management/groups.html#-GET)
  - [Suppression Group Members](https://sendgrid.com/docs/API_Reference/Web_API_v3/Suppression_Management/suppressions.html#-GET)
  - [Lists](https://sendgrid.com/docs/API_Reference/Web_API_v3/Marketing_Campaigns/contactdb.html#List-All-Lists-GET)
  - [Lists Recipients](https://sendgrid.com/docs/API_Reference/Web_API_v3/Marketing_Campaigns/contactdb.html#List-Recipients-on-a-List-GET)
  - [Segments](https://sendgrid.com/docs/API_Reference/Web_API_v3/Marketing_Campaigns/contactdb.html#List-All-Segments-GET)
  - [Segment Recipients](https://sendgrid.com/docs/API_Reference/Web_API_v3/Marketing_Campaigns/contactdb.html#List-Recipients-On-a-Segment-GET)
  - [Campaigns](https://sendgrid.com/docs/API_Reference/Web_API_v3/Marketing_Campaigns/campaigns.html#Get-all-Campaigns-GET)
  - [Templates](https://sendgrid.com/docs/API_Reference/Web_API_v3/Transactional_Templates/templates.html#-GET)
  - [Invalid Emails](https://sendgrid.com/docs/API_Reference/Web_API_v3/invalid_emails.html#List-all-invalid-emails-GET)
  - [Bounces](https://sendgrid.com/docs/API_Reference/Web_API_v3/bounces.html#List-all-bounces-GET)
  - [Blocks](https://sendgrid.com/docs/API_Reference/Web_API_v3/blocks.html#List-all-blocks-GET)
  - [Spam Reports](https://sendgrid.com/docs/API_Reference/Web_API_v3/spam_reports.html)
  - [Campaigns](https://sendgrid.com/docs/API_Reference/Web_API_v3/Marketing_Campaigns/campaigns.html#Get-all-Campaigns-GET)
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
