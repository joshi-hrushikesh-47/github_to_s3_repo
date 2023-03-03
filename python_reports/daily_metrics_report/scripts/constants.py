# import required libraries
import datetime as dt
import os

# import yaml

# opening yaml file for fetching DB details
# dir_name = os.path.abspath(os.curdir)
os.chdir("..")
dir_name = os.path.abspath(os.curdir)
dir_name = dir_name.replace('\\', '/')
dir_name = dir_name+'/'
# credentials = yaml.safe_load(open(os.path.join(dir_name, "config.yaml")))

# Calculating dates
today = dt.date.today()
yesterday = today - dt.timedelta(1)
# Second as a decimal number [00,61] (or Unix Timestamp)
# start_time = int(yesterday.strftime("%s"))
# end_time = int(today.strftime("%s"))
# report_date = yesterday
#uncomment below line (comment above one) if want to pass a manual date
report_date = dt.datetime.strptime('2021-09-17', '%Y-%m-%d')

# email_receivers_to=["pranav@porter.in", "uttamdigga@porter.in", "shrutiranjans@porter.in", "pankaj@porter.in",
#         "rizwan@porter.in", "manishgupta@porter.in ", "nitesh.shende@porter.in", "subir@porter.in",
#         "mohit.rathi@porter.in", "vijilvinayan@porter.in", "ankitdwivedi@porter.in", "cityheadsonly@porter.in", "arpanbarnwal@porter.in", "choudharyashish@porter.in", "analytics@porter.in", "product@porter.in", "growth-team@theporter.in"]
# email_cc=["shubhamagrawal@theporter.in", "kirit.bhatt@theporter.in"]
#email_receivers_to = ["kirit.bhatt@theporter.in"]
#email_cc = ["kirit.bhatt@theporter.in"]

snowflake_credentials = {
   "account": "ss29587.ap-southeast-1",
   "user": "BZ_HRUSHIKESHJ",
   "password": "Snowflake@hj12345",
   "role": "ROLE_DEV_DE",
   "database": "DEV_CODE_REPO",
   "schema": "PUBLIC",
   "warehouse": "WH_DEV_DBOPS",
}
