# Plugin - Mailchimp to S3

This plugin moves data from the [Trello](https://developers.trello.com/v1.0) API to S3. Implemented for  camapigns, connected-sites connected-sites-details, conversations,  conversations-details, lists, lists-details, reports, reports-details.
## Hooks
### MailchimpHook
This hook handles the authentication and request to Mailchimp. Based on [python-mailchimp](https://github.com/charlesthk/python-mailchimp)

### S3Hook
[Core Airflow S3Hook](https://pythonhosted.org/airflow/_modules/S3_hook.html) with the standard boto dependency.

## Operators
### MailchimpToS3Operator
This operator composes the logic for this plugin. It fetches a specific endpoint and saves the result in a S3 Bucket, under a specified key, in
njson format. The parameters it can accept include the following.

- `mailchimp_conn_id`: The Airflow id used to store the Mailchimp credentials.
- `mailchimp_resource`: The mailchimp resource we are fetching data from.
- `mailchimp_args`: S3 connection id from Airflow.  
- `s3_bucket`: The output s3 bucket.  
- `s3_key`: The input s3 key.
- `s3_bucket`: The s3 bucket where the result should be stored