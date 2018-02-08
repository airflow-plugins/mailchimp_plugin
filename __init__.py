from airflow.plugins_manager import AirflowPlugin
from mailchimp_plugin.operators.mailchimp_to_s3_operator import MailchimpToS3Operator
from mailchimp_plugin.hooks.mailchimp_hook import MailchimpHook


class mailchimp_plugin(AirflowPlugin):
    name = "mailchimp_plugin"
    operators = [MailchimpToS3Operator]
    hooks = [MailchimpHook]
    # Leave in for explicitness
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
