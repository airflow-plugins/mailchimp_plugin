import logging
import json
import collections

import urllib.request
import tarfile

from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator, SkipMixin
from airflow.utils.decorators import apply_defaults

from mailchimp_plugin.hooks.mailchimp_hook import MailchimpHook
from tempfile import NamedTemporaryFile

mappings = {
    'lists_details': {
        'parent': 'lists',
        'id_param': 'list_id'
    },
    'reports_details': {
        'parent': 'reports',
        'id_param': 'report_id'
    },
    'connected_sites_details': {
        'parent': 'connected_sites',
        'id_param': ''
    },
    'conversations_details': {
        'parent': 'conversations',
        'id_param': 'conversation_id'
    }
}


class MailchimpToS3Operator(BaseOperator, SkipMixin):
    """
    Make a query against Mailchimp and write the resulting data to s3
    """
    template_field = ('s3_key', )

    @apply_defaults
    def __init__(
        self,
        mailchimp_conn_id,
        mailchimp_resource,
        mailchimp_args={},
        s3_conn_id=None,
        s3_key=None,
        s3_bucket=None,
        *args,
        **kwargs
    ):
        """ 
        Initialize the operator
        :param mailchimp_conn_id:       name of the Airflow connection that has
                                        your Mailchimp username and api_key
        :param mailchimp_resource:      name of the Mailchimp object we are
                                        fetching data from. Implemented for 
                                            - camapigns
                                            - connected_sites
                                            - connected_sites_details
                                            - conversations
                                            - conversations_details
                                            - lists
                                            - lists_details
                                            - reports
                                            - reports_details
        :param mailchimp_args           *(optional)* dictionary with extra Mailchimp
                                        arguments
        :param s3_conn_id:              name of the Airflow connection that has
                                        your Amazon S3 conection params
        :param s3_bucket:               name of the destination S3 bucket
        :param s3_key:                  name of the destination file from bucket
        """

        super().__init__(*args, **kwargs)

        if mailchimp_resource not in ('lists_details',
                                      'reports_details',
                                      'conversations_details',
                                      'connected_sites_details',
                                      'lists',
                                      'reports',
                                      'campaigns',
                                      'conversations',
                                      'connected_sites'):
            raise Exception('Specified endpoint not currently supported.')

        self.mailchimp_conn_id = mailchimp_conn_id
        self.mailchimp_resource = mailchimp_resource
        self.mailchimp_args = mailchimp_args

        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def read_file(self, url, results_field=None):
        """
        Read the gziped response and concat results.
        """
        with NamedTemporaryFile("wb+") as tmp:
            result = list()
            urllib.request.urlretrieve(url, tmp.name)
            tar = tarfile.open(tmp.name)

            for member in tar.getmembers():
                f = tar.extractfile(member)
                if f:
                    content = f.read()
                    responses = json.loads(content.decode('utf-8'))

                    for response in responses:
                        if 'response' in response:
                            resp = json.loads(response['response'])
                           
                            if results_field:
                                result.extend(resp[results_field])
                            else:
                                result.append(resp)

            return result
    
    def get_all(self, ids, resource):
        results = []
        extra_args = {}

        for resource_id in ids:
            extra_args[mappings[resource]['id_param']] = resource_id
            result = self.hook.run_query(
                resource=mappings[resource]['parent'],
                getById=True,
                extra_args=extra_args
            )
            results += results
        
        return results
     
    def execute(self, context):
        """
        Execute the operator.
        This will get all the data for a particular Mailchimp resource
        and write it to a file.
        """
        logging.info("Prepping to gather data from Mailchimp")
        self.hook = MailchimpHook(
            conn_id=self.mailchimp_conn_id
        )

        self.hook.get_conn()

        logging.info(
            "Making request for"
            " {0} object".format(self.mailchimp_resource)
        )

        if self.mailchimp_resource in mappings:
            full_list = self.hook.run_query(
                mappings[self.mailchimp_resource]['parent']
            )
            ids = [result['id'] for result in full_list]

            results = self.get_all(ids,
                                   self.mailchimp_resource)
        elif self.mailchimp_resource in ('connected_sites', 'connected_sites_details'):
            url = self.hook.run_batch(['/connected-sites'])
            results = self.read_file(url, results_field='sites')

            if self.mailchimp_resource == 'connected_sites_details':
                endpoints = [
                    "/connected-sites/{}".format(result['id']) for result in results]
                url = self.hook.run_batch(endpoints)
                results = self.read_file(url)
        else:
            results = self.hook.run_query(self.mailchimp_resource)

        # write the results to a temporary file and save that file to s3
        if len(results) == 0 or results is None:
            logging.info("No records pulled from Mailchimp.")
            downstream_tasks = context['task'].get_flat_relatives(
                upstream=False)
            logging.info('Skipping downstream tasks...')
            logging.debug("Downstream task_ids %s", downstream_tasks)

            if downstream_tasks:
                self.skip(context['dag_run'],
                          context['ti'].execution_date,
                          downstream_tasks)
            return True

        else:
            # Write the results to a temporary file and save that file to s3.
            with NamedTemporaryFile("w") as tmp:
                for result in results:
                    filtered_result = self.filter_fields(result)
                    tmp.write(json.dumps(filtered_result) + '\n')

                tmp.flush()

                dest_s3 = S3Hook(s3_conn_id=self.s3_conn_id)
                dest_s3.load_file(
                    filename=tmp.name,
                    key=self.s3_key,
                    bucket_name=self.s3_bucket,
                    replace=True

                )
                dest_s3.connection.close()
                tmp.close()
