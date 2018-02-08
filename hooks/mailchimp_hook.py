from airflow.hooks.base_hook import BaseHook
from mailchimp3 import MailChimp
import json
import time


class MailchimpHook(BaseHook):
    def __init__(
            self,
            conn_id,
            *args,
            **kwargs):
        self.conn_id = conn_id
        self._args = args
        self._kwargs = kwargs

        self.connection = None
        self.extras = None
        self.mailchimp = None

    def run_batch(self, endpoints):
        """
        Send a batch request to mailchimp and wait to 
        be processed. Returns an url to a s3 archive.
        """
        operations = [{
            'method': 'GET',
            'path': endpoint
        } for endpoint in endpoints]

        response = self.mailchimp.batches.create(data={
            'operations': operations
        })

        batch_id = response['id']
        status = response['status']
        while response['status'] != 'finished':
            response = self.mailchimp.batches.get(batch_id=batch_id)
            time.sleep(5)
        
        return response['response_body_url']

    def get_conn(self):
        """
        Initialize a mailchimp instance.
        """
        if self.mailchimp:
            return self.mailchimp

        self.connection = self.get_connection(self.conn_id)
        self.extras = self.connection.extra_dejson

        mailchimp = MailChimp(
            self.extras['username'], self.extras['token'])
        self.mailchimp = mailchimp

        return mailchimp

    def run_query(self, resource, getById=False, extra_args={}):
        """
        Run a query against mailchimp
        :param resource:          name of the Mailchimp model
        :param getById:           True if we want to run the query
                                  for a single instance
        :param extra_args:        Mailchimp replicaton key
        """

        if 'get_all' not in extra_args and not getById:
            extra_args['get_all'] = True
        client = self.get_conn()
        mailchimp_resource = getattr(client, resource)

        result = getattr(mailchimp_resource,
                         'get' if getById else 'all')(**extra_args)
        if resource in result:
            return result[resource]

        return result

    def get_records(self, sql):
        pass

    def get_pandas_df(self, sql):
        pass

    def run(self, sql):
        pass
