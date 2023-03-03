# Load required libraries
import os
from constants import report_date, dir_name, email_receivers_to, email_cc, snowflake_credentials
from create_report import generate_report
from data_fetch import fetch_data
# from send_output_files import send_by_smtp
from sql_queries import sql_queries_declare
from transformations import transforming_data
import datetime as dt
import sys
import time
from snowflake.snowpark.session import Session
# import sahayak.message.notify as nf
#
# #arguments required for slack alert
# slack_channel = 'analytics_report_alerts' #name of the slack channel
# status = 'Daily Metrics Report - Failure msg' #Slack message title/header
# msg = 'DMR failed. {}' #Slack message with the error log
#
# #arguments for email alert
# message = """
# <html>
# <body>
# <p>Hi all<br>
# The script for today's Daily Metrics Report failed due to an exception.
# We are trying to fix the issue. Apologies for the incovenience.</p>
# </body>
# </html>
# """
# path = os.path.join(dir_name, "config.yaml")
# email_subject = 'Script Failure notification! - Daily Metrics report'
#
# @nf.slack_email_notifier(slack_channel=slack_channel, status=status, msg=msg, to = email_receivers_to, cc = email_cc, subject = email_subject, message = message, config_path = path)
# def execute_script(session, report_date, dir_name):
def execute_script(session, report_date, env_json):
    try:
        start_time = time.time()
        report_date_str = report_date
        report_date = dt.datetime.strptime(report_date, '%Y-%m-%d')
        # Creating Snowflake session object
        # session = Session.builder.configs(snowflake_credentials).create()

        # Getting sql queries from sql_queries module by calling sql_queries_declare module
        sql_queries_dictionary = sql_queries_declare(report_date, env_json)

        # Fetching data and saving data and saving into DFs
        raw_bases = fetch_data(sql_queries_dictionary, session)

        # calling transforming_data function. This will return final dataframes for the
        # Demand and Supply sheets
        demand_final, supply_final, response = transforming_data(raw_bases, session)
        # response = transforming_data(raw_bases, session)
        if response != 'Transformation succeded!':
            return response

        # Calling below function generates an excel file having Demand and Supply Metrics
        generate_report(session, demand_final, supply_final, report_date_str)

        # # send report
        # files_to_attach = [os.path.join(dir_name, "scripts/daily_metrics_report/daily_report/daily_metrics_report_{report_date}.xlsx".format(
        #     report_date=report_date.strftime("%Y-%m-%d")))]
        # print("Send an email using Python")
        #
        # result = send_by_smtp(
        #     to=email_receivers_to,
        #     cc= email_cc,
        #     subject="Daily Metrics Report",
        #     attachments=files_to_attach, attachment_type="html",)
        # if result:
        #     print("Email Sent Successfully")
        # else:
        #     print("Email Sending Failed")
        end_time = time.time()
        total_time = end_time - start_time
        return f"Successfully executed Pandas code in Snowflake in {total_time} seconds!"

    except Exception as e:
        print(e)
        exception_type, exception_object, exception_traceback = sys.exc_info()
        filename = exception_traceback.tb_frame.f_code.co_filename
        line_number = exception_traceback.tb_lineno
        response_obj = {
            'exception_type': exception_type,
            'error': e,
            'filename': filename,
            'line_number': line_number
        }
        return str(response_obj)

# execute_script(report_date, dir_name)
