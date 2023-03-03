# importing required modules
from functools import reduce

import sys
import numpy as np
import pandas as pd
from user_functions import write_to_db
from constants import report_date, snowflake_credentials
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def transforming_data(raw_bases, session):
    try:
        orders_base_raw = list(raw_bases.values())[0]
        geos_base_raw = list(raw_bases.values())[1]
        cancel_reason_base = list(raw_bases.values())[2]
        customers_first_order_base = list(raw_bases.values())[3]
        first_app_order_date_base = list(raw_bases.values())[4]
        order_fares_base = list(raw_bases.values())[5]
        order_location_base = list(raw_bases.values())[6]
        drivers_base_raw = list(raw_bases.values())[7]
        login_time_base_raw = list(raw_bases.values())[8]
        engagement_shift_timings_base_raw = list(raw_bases.values())[9]
        completed_base_raw = list(raw_bases.values())[10]

        # Demand Sheet
        # creating dataframes as per defined conditions
        orders_base = pd.merge(orders_base_raw, geos_base_raw, left_on='GEO_REGION_ID',
                               right_on='ID_GEOS', how='inner', suffixes=('_ORDERS', '_GEOS'))

        completed_base = pd.merge(completed_base_raw, orders_base, left_on='ORDER_ID',
                               right_on='ID_ORDERS', how='left', suffixes=('_COMP', '_ORD'))
        completed_base = completed_base.astype({'CUSTOMER_FARE_WITHOUT_PREVIOUS_DUE': float})
        cancelled_base = orders_base[orders_base['STATUS'] == 5]
        cb_join_cr = pd.merge(cancelled_base, cancel_reason_base, left_on='CANCEL_REASON_ID',
                              right_on='ID', how='inner', suffixes=('_PC', '_CR'))
        porter_cancelled_base = cb_join_cr[(cb_join_cr['STATUS_CR'] == 0) & (
            cb_join_cr.PORTER_ACCOUNTABLE == 't')]

        # calculating metrics for the daily metric report demand sheet

        # completed order count georegion wise
        completed = completed_base.groupby(
            ['GEO_REGION_ID', 'NAME']).size().reset_index(name='COMPLETED')

        # cancelled order count georegion wise
        cancelled = cancelled_base.groupby(
            'GEO_REGION_ID').size().reset_index(name='CANCELLED')

        # porter accountable cancellation order count georegion wise
        porter_cancellation = porter_cancelled_base.groupby(
            'GEO_REGION_ID').size().reset_index(name='PORTER CANCELLATIONS')

        # Cancellation percentage georegion wise
        completed_join_cancelled = pd.merge(completed, cancelled, left_on='GEO_REGION_ID',
                                            right_on='GEO_REGION_ID', how='left', suffixes=('_COMP', '_CANC'))
        completed_join_cancelled['CANCELLATION_PERCENTAGE'] = (((completed_join_cancelled['CANCELLED'])*100) / (
            completed_join_cancelled['CANCELLED'] + completed_join_cancelled['COMPLETED'])).fillna(0)
        completed_join_cancelled['CANCELLATION_PERCENTAGE'] = completed_join_cancelled['CANCELLATION_PERCENTAGE'].replace([
            np.inf, -np.inf], 0)
        cancellation_percentage = completed_join_cancelled.drop(
            columns=['COMPLETED', 'CANCELLED', 'NAME'])

        # CC (customer care placed) Order count georegion wise
        cc_orders = completed_base[completed_base['SOURCE'] == 0].groupby(
            'GEO_REGION_ID').size().reset_index(name='CC ORDERS')

        # App order count (ios and android) georegion wise
        app_orders = completed_base[completed_base['SOURCE'].isin([2, 3])].groupby(
            'GEO_REGION_ID').size().reset_index(name='APP ORDERS')

        # FOS orders (Orders taken by Field agents) count georegion wise
        fos_orders = completed_base[completed_base['FOS_ID'].notnull()].groupby(
            'GEO_REGION_ID').size().reset_index(name='FOS CUSTOMER ORDERS')

        # Organic order count georegion wise
        organic_orders = completed_base[completed_base['FOS_ID'].isnull()].groupby(
            'GEO_REGION_ID').size().reset_index(name='ORGANIC CUSTOMER ORDERS')

        # Total Net Revenue georegion wise
        net_revenues = completed_base.groupby('GEO_REGION_ID')[
            'CUSTOMER_FARE_WITHOUT_PREVIOUS_DUE'].sum().reset_index(name='NET REVENUE')

        # Calculating gross revenue georegion wise
        completed_join_order_fares = pd.merge(
            completed_base, order_fares_base, left_on='ID_ORDERS', right_on='ORDER_ID', how='inner', suffixes=('_COMP', '_OF'))

        completed_join_order_fares = completed_join_order_fares.astype({'PREMIUM_DISCOUNT': float,
                                                                        'RETURN_DISCOUNT': float,
                                                                        'REFERRAL_DISCOUNT': float,
                                                                        'COUPON_DISCOUNT': float,
                                                                        'TRAVEL_DISTANCE': float,
                                                                        'TRAVEL_DURATION': float})

        completed_join_order_fares['GROSS_REVENUE'] = (completed_join_order_fares['CUSTOMER_FARE_WITHOUT_PREVIOUS_DUE']
                                                       + completed_join_order_fares['PREMIUM_DISCOUNT'] +
                                                       completed_join_order_fares['RETURN_DISCOUNT'] +
                                                       completed_join_order_fares['REFERRAL_DISCOUNT'] +
                                                       completed_join_order_fares['COUPON_DISCOUNT']).fillna(0)
        gross_revenues = completed_join_order_fares.groupby(
            'GEO_REGION_ID')['GROSS_REVENUE'].sum().reset_index(name='GROSS REVENUE')

        # Calculating avg ticket size georegion wise
        avg_ticket_size = completed_base.groupby('GEO_REGION_ID').agg(
            AVG_TICKET_SIZE=('CUSTOMER_FARE_WITHOUT_PREVIOUS_DUE', 'mean')).reset_index()

        # Calculating avg trip distance and duration georegion wise
        avg_trip_distance_duration = completed_join_order_fares[completed_join_order_fares['DRIVER_ID'].notnull()].groupby(
            'GEO_REGION_ID').agg(AVG_TRIP_DISTANCE=('TRAVEL_DISTANCE', 'mean'), AVG_TRIP_DURATION=('TRAVEL_DURATION', 'mean')).reset_index()

        # Calculating avg dry run georegion wise
        completed_join_order_location = pd.merge(
            completed_base, order_location_base, left_on='ID_ORDERS', right_on='ORDER_ID', how='inner', suffixes=('_COMP', '_OL'))

        completed_join_order_location = completed_join_order_location.astype({'ACCEPT_START_TRIP_DISTANCE': float})

        avg_dry_run = completed_join_order_location[(completed_join_order_location['DRIVER_ID'].notnull()
                                                     & completed_join_order_location['ACCEPT_START_TRIP_DISTANCE'].notnull(
        ))].groupby('GEO_REGION_ID').agg(AVG_DRY_RUN=('ACCEPT_START_TRIP_DISTANCE', 'mean')).reset_index()

        # Calculating delay metrics georegion wise
        driver_to_leave_accept_vicinity = 10*60  # 600 seconds (10 minutes)
        delayed_trips = completed_base[((completed_base['ACCEPT_VICINITY_TIMESTAMP'] -
                                        completed_base['TRIP_ACCEPTED_TIME']) >= driver_to_leave_accept_vicinity)]

        delayed_15mins = delayed_trips[((delayed_trips['TRIP_START_ENTRY_TIMESTAMP'] - delayed_trips['PICKUP_TIME']) >= 0) & (
            (delayed_trips['TRIP_START_ENTRY_TIMESTAMP'] - delayed_trips['PICKUP_TIME']) < (15*60))].groupby('GEO_REGION_ID').size().reset_index(name='DELAYED_15MINS')

        delayed_30mins = delayed_trips[((delayed_trips['TRIP_START_ENTRY_TIMESTAMP'] - delayed_trips['PICKUP_TIME']) >= (15*60)) & (
            (delayed_trips['TRIP_START_ENTRY_TIMESTAMP'] - delayed_trips['PICKUP_TIME']) < (30*60))].groupby('GEO_REGION_ID').size().reset_index(name='DELAYED_30MINS')

        delayed_60mins = delayed_trips[((delayed_trips['TRIP_START_ENTRY_TIMESTAMP'] - delayed_trips['PICKUP_TIME']) >= (30*60)) & (
            (delayed_trips['TRIP_START_ENTRY_TIMESTAMP'] - delayed_trips['PICKUP_TIME']) < (60*60))].groupby('GEO_REGION_ID').size().reset_index(name='DELAYED_60MINS')

        delayed_1hour = delayed_trips[((delayed_trips['TRIP_START_ENTRY_TIMESTAMP'] - delayed_trips['PICKUP_TIME']) >= (60*60)) & (
            (delayed_trips['TRIP_START_ENTRY_TIMESTAMP'] - delayed_trips['PICKUP_TIME']) < (365*24*60*60))].groupby('GEO_REGION_ID').size().reset_index(name='DELAYED_BY1HOUR')

        # Calculating unique customer count georegion wise
        unique_customers = completed_base.groupby(['GEO_REGION_ID']).agg(
            UNIQUE_CUSTOMERS=("CUSTOMER_ID", "nunique")).reset_index()

        # Calculating first time app user count georegion wise
        first_time_app_user_count = first_app_order_date_base[first_app_order_date_base['FIRST_APP_ORDER_DATE'] == str(
            report_date.strftime("%Y-%m-%d"))].groupby('GEO_REGION_ID').agg(FIRST_TIME_APP_USERS=("CUSTOMER_ID", "nunique")).reset_index()

        # Calculating order count by new customers georegion wise
        completed_join_firstorder = pd.merge(completed_base, customers_first_order_base,
                                             left_on='CUSTOMER_ID', right_on='CUSTOMER_ID', how='inner', suffixes=('_COMP', '_FO'))
        by_new_customer_order_count = completed_join_firstorder.groupby(
            'GEO_REGION_ID_COMP').agg(BY_NEW_CUSTOMER=("ID_ORDERS", "nunique")).reset_index()
        by_new_customer_order_count.rename(
            {'GEO_REGION_ID_COMP': 'GEO_REGION_ID'}, axis=1, inplace=True)

        # Calculating order count by old customers georegion wise
        by_old_customer_order_count_inter = completed_base.drop_duplicates().merge(customers_first_order_base.drop_duplicates(),
                                                                                   on=customers_first_order_base.columns.to_list(), how='left', indicator=True)
        by_old_customer_order_count_inter = by_old_customer_order_count_inter.loc[
            by_old_customer_order_count_inter._merge == 'left_only', by_old_customer_order_count_inter.columns != '_merge']
        by_old_customer_order_count = by_old_customer_order_count_inter.groupby(
            'GEO_REGION_ID').size().reset_index(name='BY_OLD_CUSTOMER')

        # Calculating unfulfilled/cancelled order count by new customers georegion wise
        cancelled_join_firstorder = pd.merge(cancelled_base, customers_first_order_base,
                                             left_on='CUSTOMER_ID', right_on='CUSTOMER_ID', how='inner', suffixes=('_CANC', '_FO'))
        unfulfilled_new_customer_order_count = cancelled_join_firstorder.groupby(
            'GEO_REGION_ID_CANC').size().reset_index(name='UNFULFILLED_NEW_CUSTOMER')
        unfulfilled_new_customer_order_count.rename(
            {'GEO_REGION_ID_CANC': 'GEO_REGION_ID'}, axis=1, inplace=True)

        ################################################################################################################################################################################################################################################################################################################

        # Supply Sheet
        # Calculate unique drivers count who completed orders geo region wise
        unique_drivers_on_orders = completed_base.groupby('GEO_REGION_ID').agg(
            UNIQUE_DRIVERS_ON_ORDERS=('DRIVER_ID', 'nunique')).reset_index()

        # Calculate new drivers count who enrolled on report date geo region wise
        new_drivers = drivers_base_raw[(drivers_base_raw['DATE_OF_JOIN'] == report_date.strftime(
            "%Y-%m-%d"))].groupby(['GEO_REGION_ID']).agg(NEW_DRIVERS=('ID', 'nunique')).reset_index()

        # calculate order count per driver
        orders_join_drivers = pd.merge(completed_base, drivers_base_raw,
                                       left_on='DRIVER_ID', right_on='ID', how='inner', suffixes=('_ORD', '_DR'))
        orders_per_driver = orders_join_drivers.groupby(['GEO_REGION_ID_ORD', 'DRIVER_ID']).agg(
            ORDERS_COUNT=('ID_ORDERS', 'nunique')).reset_index()
        orders_per_driver.rename(
            {'GEO_REGION_ID_ORD': 'GEO_REGION_ID'}, axis=1, inplace=True)

        # Driver counts based on completed order counts georegion wise
        with_1order = orders_per_driver[orders_per_driver['ORDERS_COUNT'] == 1].groupby(
            'GEO_REGION_ID').agg(WITH_1ORDER=('DRIVER_ID', 'nunique')).reset_index()
        with_2orders = orders_per_driver[orders_per_driver['ORDERS_COUNT'] == 2].groupby(
            'GEO_REGION_ID').agg(WITH_2ORDERS=('DRIVER_ID', 'nunique')).reset_index()
        with_3_and_more_orders = orders_per_driver[orders_per_driver['ORDERS_COUNT'] >= 3].groupby(
            'GEO_REGION_ID').agg(WITH_3_AND_MORE_ORDERS=('DRIVER_ID', 'nunique')).reset_index()

        # creating login_time and engagement_shift_time base
        login_time_base = login_time_base_raw
        engagement_shift_timings_base = engagement_shift_timings_base_raw

        # Calculating engagement start and time for each georegion
        engagement_shift_timings_base['ENG_START_INTERVAL'] = engagement_shift_timings_base['ENGAGEMENT_START_HOUR'].astype(
            str) + ":" + engagement_shift_timings_base['ENGAGEMENT_START_MINUTES'].astype(str)
        engagement_shift_timings_base['ENG_START_INTERVAL'] = pd.to_datetime(
            engagement_shift_timings_base['ENG_START_INTERVAL'], format='%H:%M').dt.strftime('%H:%M:%S')
        engagement_shift_timings_base['ENG_END_INTERVAL'] = engagement_shift_timings_base['ENGAGEMENT_END_HOUR'].astype(
            str) + ":" + engagement_shift_timings_base['ENGAGEMENT_END_MINUTES'].astype(str)
        engagement_shift_timings_base['ENG_END_INTERVAL'] = pd.to_datetime(
            engagement_shift_timings_base['ENG_END_INTERVAL'], format='%H:%M').dt.strftime('%H:%M:%S')

        # joining login time and engagement time
        logintime_join_engtime = pd.merge(login_time_base, engagement_shift_timings_base,
                                          left_on='GEO_REGION_ID', right_on='GEO_REGION_ID', how='inner', suffixes=('_LOG', '_ENG'))
        logintime_join_engtime = logintime_join_engtime.drop(
            columns=['ENGAGEMENT_END_HOUR', 'ENGAGEMENT_END_MINUTES', 'ENGAGEMENT_START_HOUR', 'ENGAGEMENT_START_MINUTES'])

        logintime_join_engtime['ENG_END_INTERVAL'] = pd.to_datetime(logintime_join_engtime['ENG_END_INTERVAL'], format= '%H:%M:%S').dt.time
        logintime_join_engtime['ENG_START_INTERVAL'] = pd.to_datetime(logintime_join_engtime['ENG_START_INTERVAL'], format= '%H:%M:%S').dt.time
        # filtering for logins under engagement time shifts
        login_hours_base = logintime_join_engtime[(logintime_join_engtime['ENG_START_INTERVAL'] <= logintime_join_engtime['START_INTERVAL']) &
                                                  (logintime_join_engtime['START_INTERVAL'] <= logintime_join_engtime['ENG_END_INTERVAL']) &
                                                  (logintime_join_engtime['ENG_START_INTERVAL'] <= logintime_join_engtime['END_INTERVAL']) &
                                                  (logintime_join_engtime['END_INTERVAL'] <= logintime_join_engtime['ENG_END_INTERVAL'])]
        # calculating driver_id level total login time
        login_time = login_hours_base.groupby(['GEO_REGION_ID', 'DRIVER_ID']).agg(
            TOTAL_LOGIN_TIME=('TOTAL_LOGIN', 'sum')).reset_index()
        # creating login time column in mins
        login_time['TOTAL_LOGIN_TIME_IN_MINS'] = login_time['TOTAL_LOGIN_TIME'] * 60
        login_time = login_time.astype({'TOTAL_LOGIN_TIME_IN_MINS': float,
                                        'TOTAL_LOGIN_TIME': float})
        # filtering for logins more than or equal to 30 mins (valid logins)
        login_time = login_time[login_time['TOTAL_LOGIN_TIME_IN_MINS'] >= 30]

        # calculating avg login hours georegion wise
        avg_login_hrs = login_time.groupby('GEO_REGION_ID').agg(
            AVG_LOGIN_HRS=('TOTAL_LOGIN_TIME', 'mean')).reset_index()
        # calculating loggedin drivers count georegion wise
        logged_in_drivers = login_time.groupby('GEO_REGION_ID').agg(
            LOGGED_IN_DRIVERS=('DRIVER_ID', 'nunique')).reset_index()
        # calculating loggedin drivers count (logintime more than 4 hours) georegion wise
        logged_in_4hours = login_time[login_time['TOTAL_LOGIN_TIME'] >= 4].groupby(
            'GEO_REGION_ID').agg(LOGGED_IN_4HOURS=('DRIVER_ID', 'nunique')).reset_index()
        # calculating loggedin drivers count (logintime more than 6 hours) georegion wise
        logged_in_6hours = login_time[login_time['TOTAL_LOGIN_TIME'] >= 6].groupby(
            'GEO_REGION_ID').agg(LOGGED_IN_6HOURS=('DRIVER_ID', 'nunique')).reset_index()
        # calculating loggedin drivers count (logintime more than 8 hours) georegion wise
        logged_in_8hours = login_time[login_time['TOTAL_LOGIN_TIME'] >= 8].groupby(
            'GEO_REGION_ID').agg(LOGGED_IN_8HOURS=('DRIVER_ID', 'nunique')).reset_index()

        # getting driverids with zero orders
        common_drivers = drivers_base_raw.merge(
            orders_per_driver, left_on=["ID"], right_on=["DRIVER_ID"])
        zero_order_drivers = drivers_base_raw[~drivers_base_raw['ID'].isin(
            common_drivers['DRIVER_ID'])]

        # Getting driver count with logintime more than 4 hours and zero orders georegion wise
        loggedin4hrs_zero_orders = login_time[(login_time['DRIVER_ID'].isin(zero_order_drivers['ID'])) & (
            login_time['TOTAL_LOGIN_TIME'] > 4)].groupby('GEO_REGION_ID').agg(LOGGEDIN4HRS_ZERO_ORDERS=('DRIVER_ID', 'nunique')).reset_index()
        # Getting driver count with logintime more than 6 hours and zero orders georegion wise
        loggedin6hrs_zero_orders = login_time[(login_time['DRIVER_ID'].isin(zero_order_drivers['ID'])) & (
            login_time['TOTAL_LOGIN_TIME'] > 6)].groupby('GEO_REGION_ID').agg(LOGGEDIN6HRS_ZERO_ORDERS=('DRIVER_ID', 'nunique')).reset_index()
        # Getting driver count with logintime more than 8 hours and zero orders georegion wise
        loggedin8hrs_zero_orders = login_time[(login_time['DRIVER_ID'].isin(zero_order_drivers['ID'])) & (
            login_time['TOTAL_LOGIN_TIME'] > 8)].groupby('GEO_REGION_ID').agg(LOGGEDIN8HRS_ZERO_ORDERS=('DRIVER_ID', 'nunique')).reset_index()

        # Demand Sheet
        # merge dfs
        demand_agg = [completed, cancelled, porter_cancellation, cancellation_percentage, cc_orders,
                      app_orders, fos_orders, organic_orders, by_old_customer_order_count, by_new_customer_order_count,
                      unfulfilled_new_customer_order_count, net_revenues, gross_revenues, avg_ticket_size,
                      avg_trip_distance_duration, avg_dry_run, delayed_15mins, delayed_30mins, delayed_60mins,
                      delayed_1hour, unique_customers, first_time_app_user_count]
        demand_final = reduce(lambda left, right: pd.merge(
            left, right, on='GEO_REGION_ID', how='outer'), demand_agg)

        # formatting changes
        demand_final = demand_final.fillna(0)
        demand_final = np.round(demand_final, decimals=2)
        demand_final['REPORT_DATE'] = report_date.strftime("%Y-%m-%d")  # adding report date column
        demand_final = pd.merge(demand_final, geos_base_raw, left_on='GEO_REGION_ID',
                                right_on='ID_GEOS', how='inner', suffixes=('_DF', '_GEOS'))
        demand_final = demand_final.drop(columns=['ID_GEOS', 'NAME_DF'])
        demand_final = demand_final[['GEO_REGION_ID', 'NAME_GEOS', 'REPORT_DATE', 'COMPLETED', 'CANCELLED',
                                     'PORTER CANCELLATIONS', 'CANCELLATION_PERCENTAGE', 'CC ORDERS', 'APP ORDERS', 'FOS CUSTOMER ORDERS',
                                     'ORGANIC CUSTOMER ORDERS', 'BY_OLD_CUSTOMER', 'BY_NEW_CUSTOMER', 'UNFULFILLED_NEW_CUSTOMER',
                                     'NET REVENUE', 'GROSS REVENUE', 'AVG_TICKET_SIZE', 'AVG_TRIP_DISTANCE', 'AVG_DRY_RUN',
                                     'AVG_TRIP_DURATION', 'DELAYED_15MINS', 'DELAYED_30MINS', 'DELAYED_60MINS',
                                     'DELAYED_BY1HOUR', 'UNIQUE_CUSTOMERS', 'FIRST_TIME_APP_USERS']]  # reorder columns
        demand_final.columns = ['GEO_REGION_ID', 'CITY_NAME', 'REPORT_DATE', 'COMPLETED', 'CANCELLED',
                                     'PORTER_CANCELLATIONS', 'CANCELLATION_PERCENTAGE', 'CC_ORDERS', 'APP_ORDERS', 'FOS_CUSTOMER_ORDERS',
                                     'ORGANIC_CUSTOMER_ORDERS', 'BY_OLD_CUSTOMER', 'BY_NEW_CUSTOMER', 'UNFULFILLED_NEW_CUSTOMERS',
                                     'NET_REVENUE', 'GROSS_REVENUE', 'AVG_TICKET_SIZE', 'AVG_TRIP_DISTANCE', 'AVG_DRY_RUN',
                                     'AVG_TRIP_DURATION', 'DELAYED_0_15_MINS', 'DELAYED_15_30_MINS', 'DELAYED_30_60_MINS',
                                     'DELAYED_BY_1_HOUR', 'UNIQUE_CUSTOMERS', 'FIRST_TIME_APP_USERS']

        # #writing data to redshift table
        # write_to_db('dmr_demand_daily', demand_final, 'redshift_db', 'append')

        # Writing data into Snowflake table
        # with snowflake.connector.connect(
        #         user=snowflake_credentials['user'],
        #         password=snowflake_credentials['password'],
        #         account=snowflake_credentials['account'],
        #         warehouse=snowflake_credentials['warehouse'],
        #         database=snowflake_credentials['database'],
        #         schema=snowflake_credentials['schema']
        # ) as cnx:
        # success, nchunks, nrows, copy_output = write_pandas(session, demand_final, 'DMR_DEMAND_DAILY')
        #
        # print('\nWriting into table dmr_demand_daily-')
        # print('success = ', success)
        # print('nchunks = ', nchunks)
        # print('nrows = ', nrows)
        # print('copy_output = ', copy_output)

        # demand_final_sp_df = session.write_pandas(demand_final, 'DMR_DEMAND_DAILY', overwrite=False)
        demand_final_sp_df = session.createDataFrame(demand_final)
        demand_final_sp_df = demand_final_sp_df.write.save_as_table('DMR_DEMAND_DAILY', mode='append')

        demand_final.columns = ['GeoRegion', 'City', 'Report Date', 'Completed', 'Cancelled', 'Porter Cancellations',
                                'Cancellation Percentage', 'CC Orders', 'App Orders', 'FOS Customer Orders', 'Organic Customer Orders',
                                'By Old Customers', 'By New Customers', 'Unfulfilled new customers', 'Net revenue', 'Gross revenue',
                                'Avg Ticket Size', 'Avg Trip Distance', 'Avg Dry Run', 'Avg Trip Time', 'Delayed 0-15 Mins',
                                'Delayed 15-30 Mins', 'Delayed by 30-60 Mins', 'Delayed by 1 hour', 'Unique Customers',
                                'First-time App Users']  # rename columns

        # merge dfs
        supply_agg = [unique_drivers_on_orders, new_drivers, with_1order, with_2orders, with_3_and_more_orders,
                      avg_login_hrs, logged_in_drivers, logged_in_4hours, logged_in_6hours, logged_in_8hours,
                      loggedin4hrs_zero_orders, loggedin6hrs_zero_orders, loggedin8hrs_zero_orders]
        supply_final = reduce(lambda left, right: pd.merge(
            left, right, on='GEO_REGION_ID', how='outer'), supply_agg)

        # formatting changes
        supply_final = supply_final.fillna(0)
        supply_final = np.round(supply_final, decimals=2)
        supply_final['REPORT_DATE'] = report_date.strftime("%Y-%m-%d")  # adding report date column
        supply_final = pd.merge(supply_final, geos_base_raw, left_on='GEO_REGION_ID',
                                right_on='ID_GEOS', how='inner', suffixes=('_df', '_geos'))
        supply_final = supply_final.drop(columns=['ID_GEOS'])
        supply_final = supply_final[['GEO_REGION_ID', 'NAME', 'REPORT_DATE', 'UNIQUE_DRIVERS_ON_ORDERS',
                                     'NEW_DRIVERS', 'WITH_1ORDER', 'WITH_2ORDERS', 'WITH_3_AND_MORE_ORDERS', 'AVG_LOGIN_HRS',
                                     'LOGGED_IN_DRIVERS', 'LOGGED_IN_4HOURS', 'LOGGED_IN_6HOURS', 'LOGGED_IN_8HOURS',
                                     'LOGGEDIN4HRS_ZERO_ORDERS', 'LOGGEDIN6HRS_ZERO_ORDERS', 'LOGGEDIN8HRS_ZERO_ORDERS']]  # reorder columns

        supply_final.columns = ['GEO_REGION_ID', 'CITY_NAME', 'REPORT_DATE', 'UNIQUE_DRIVERS_ON_ORDERS',
                                     'NEW_DRIVERS', 'WITH_1_ORDER', 'WITH_2_ORDERS', 'WITH_3_AND_MORE_ORDERS', 'AVG_LOGIN_HRS',
                                     'LOGGED_IN_DRIVERS', 'LOGGED_IN_4HOURS', 'LOGGED_IN_6HOURS', 'LOGGED_IN_8HOURS',
                                     'LOGGEDIN4HRS_ZERO_ORDERS', 'LOGGEDIN6HRS_ZERO_ORDERS', 'LOGGEDIN8HRS_ZERO_ORDERS']

        # #writing data to redshift table
        # write_to_db('dmr_supply_daily', supply_final, 'redshift_db', 'append')

        # Writing data into Snowflake table
        # with snowflake.connector.connect(
        #         user=snowflake_credentials['user'],
        #         password=snowflake_credentials['password'],
        #         account=snowflake_credentials['account'],
        #         warehouse=snowflake_credentials['warehouse'],
        #         database=snowflake_credentials['database'],
        #         schema=snowflake_credentials['schema']
        # ) as cnx:
        # success, nchunks, nrows, copy_output = write_pandas(session, supply_final, 'DMR_SUPPLY_DAILY')
        #
        # print('\nWriting into table dmr_supply_daily-')
        # print('success = ', success)
        # print('nchunks = ', nchunks)
        # print('nrows = ', nrows)
        # print('copy_output = ', copy_output)

        # supply_final_sp_df = session.write_pandas(supply_final, 'DMR_SUPPLY_DAILY', overwrite=False)
        supply_final_sp_df = session.createDataFrame(supply_final)
        supply_final_sp_df = supply_final_sp_df.write.save_as_table('DMR_SUPPLY_DAILY', mode='append')

        supply_final.columns = ['GeoRegion', 'City', 'Report Date', 'Unique Drivers on Order', '# New Drivers',
                                'With 1 Order', 'With 2 Orders', 'With >= 3 Orders', 'Avg Login Hrs', 'Logged in drivers (min 30 mins)',
                                'Logged in > 4 Hrs', 'Logged in > 6 Hrs', 'Logged in > 8 Hrs', 'Logged in > 4 Hrs & zero orders',
                                'Logged in > 6 Hrs & zero orders', 'Logged in > 8 Hrs & zero orders']  # rename columns

        # return demand_final, supply_final
        return demand_final, supply_final, 'Transformation succeded!'

    except Exception as e:
        print('\nERROR! -> ', e)
        exception_type, exception_object, exception_traceback = sys.exc_info()
        filename = exception_traceback.tb_frame.f_code.co_filename
        line_number = exception_traceback.tb_lineno

        print("Exception type: ", exception_type)
        print("File name: ", filename)
        print("Line number: ", line_number)
        response_obj = {
            'exception_type': exception_type,
            'error': e,
            'filename': filename,
            'line_number': line_number
        }
        return str(response_obj)
