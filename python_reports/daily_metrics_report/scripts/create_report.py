# importing required modules
import os

import pandas as pd

from user_functions import adjust_column_width
from snowflake.snowpark.session import Session
from snowflake import snowpark

#Function for generating excel report
def generate_report(session, demand_final, supply_final, report_date):

    demand_output = session.sql(f'copy into @DEV_CODE_REPO.COM.EXT_PORTER_POC_STAGE/code/sf_test_report_script/Snowflake_reports/daily_metrics_report/report_files/daily_metrics_demand_report_{report_date} from DMR_DEMAND_DAILY FILE_FORMAT = (TYPE = CSV COMPRESSION = NONE) OVERWRITE = TRUE HEADER = TRUE;').collect()

    supply_output = session.sql(f'copy into @DEV_CODE_REPO.COM.EXT_PORTER_POC_STAGE/code/sf_test_report_script/Snowflake_reports/daily_metrics_report/report_files/daily_metrics_supply_report_{report_date} from DMR_SUPPLY_DAILY  FILE_FORMAT = (TYPE = CSV COMPRESSION = NONE) OVERWRITE = TRUE HEADER = TRUE;').collect()

    # # writing an excel file
    # writer = pd.ExcelWriter(os.path.join(dir_name,"daily_report/daily_metrics_report_{report_date}.xlsx".format(
    #     report_date=report_date.strftime("%Y-%m-%d"))))
    # demand_final.to_excel(writer, sheet_name='Demand', index=False)
    #
    # # auto adjust excel column width
    # adjust_column_width(demand_final, 'Demand', writer)
    # demand_final = demand_final.style.set_properties(
    #     **{'text-align': 'center'})
    # demand_final.to_excel(writer, sheet_name='Demand', index=False)
    #
    # # writing to same excel as Demand sheet
    # supply_final.to_excel(writer, sheet_name='Supply', index=False)
    #
    # # auto adjust excel column width
    # adjust_column_width(supply_final, 'Supply', writer)
    #
    # supply_final = supply_final.style.set_properties(
    #     **{'text-align': 'center'})
    # supply_final.to_excel(writer, sheet_name='Supply', index=False)
    #
    # writer.save()
    print("Report Generated Sucessfully")
