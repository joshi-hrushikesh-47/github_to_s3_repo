from datetime import time, timedelta, timezone
import json
import ast

# Declaring SNOWFLAKE sql_queries
def sql_queries_declare(report_date, env_json):

    env_dict = ast.literal_eval(env_json)
    dev_curated_db_name = env_dict['dev_curated_db_name']
    oms_public_schema_name = env_dict['oms_public_schema_name']

    # sql queries for retrieving raw data from different tables
    # Demand sheet
    order_bs_sql = f"""
                    SELECT
                        id as id_orders
                        , geo_region_id
                        , status
                        , trip_ended_time
                        , cancel_reason_id
                        , source
                        , fos_id
                        , customer_fare_without_previous_due
                        , driver_id
                        , accept_vicinity_timestamp
                        , trip_accepted_time
                        , trip_start_entry_timestamp
                        , pickup_time
                        , customer_id
                    FROM {dev_curated_db_name}.{oms_public_schema_name}.orders
                    WHERE 1=1
                    and (pickup_time >= date_part(epoch_second, to_timestamp_tz('{report_date.strftime("%Y-%m-%d")}'))::INT
                         AND pickup_time < date_part(epoch_second, to_timestamp_tz('{(report_date + timedelta(days=1)).strftime("%Y-%m-%d")}'))::INT)
                    and order_type = 0
                    and deleted_at is null
                    OR (
                        order_type = 0
                        and deleted_at is null
                        and (trip_ended_time >= date_part(epoch_second, to_timestamp_tz('{report_date.strftime("%Y-%m-%d")}'))::INT
                            AND trip_ended_time < date_part(epoch_second, to_timestamp_tz('{(report_date + timedelta(days=1)).strftime("%Y-%m-%d")}'))::INT
                        )
                        )
                """

    geos_sql = f"""SELECT distinct id as id_geos, name FROM {dev_curated_db_name}.{oms_public_schema_name}.geo_regions"""

    cancel_reason_sql = f"""
                        SELECT
                        distinct id
                        , name as name_cr
                        , status as status_cr
                        , porter_accountable
                        FROM {dev_curated_db_name}.{oms_public_schema_name}.cancel_reasons
                        """

    customers_first_order_sql = f"""
                                SELECT
                                distinct id as customer_id
                                , geo_region_id
                                FROM {dev_curated_db_name}.{oms_public_schema_name}.customers
                                Where 1=1
                                and first_order_time is not null
                                and (first_order_time >= date_part(epoch_second, to_timestamp_tz('{report_date.strftime("%Y-%m-%d")}'))::INT
                                AND first_order_time < date_part(epoch_second, to_timestamp_tz('{(report_date + timedelta(days=1)).strftime("%Y-%m-%d")}'))::INT)
                                """

    first_app_order_date_sql = f"""
                            SELECT
                            customer_id
                            , geo_region_id
                            , min(date(to_timestamp_tz(pickup_time))) as first_app_order_date
                            FROM {dev_curated_db_name}.{oms_public_schema_name}.orders
                            WHERE 1=1
                            and status = 4 --completed orders only
                            and order_type = 0
                            and deleted_at is null
                            and tele_agent_id is null
                            --and intercity_flag = 0
                            and source in (2,3) --app orders ios and android
                            Group by 1, 2
                            """

    # SQL Query for retrieving order fare data for completed order_ids
    order_fares_sql = f"""SELECT  
                          fares.id
                        , fares.order_id
                        , fares.fare_type
                        , fares.premium_discount
                        , fares.return_discount
                        , fares.referral_discount
                        , fares.coupon_discount
                        , fares.travel_distance
                        , fares.travel_duration
                        FROM {dev_curated_db_name}.{oms_public_schema_name}.order_fares fares
                        join {dev_curated_db_name}.{oms_public_schema_name}.completed_spot_orders_fast_mv cmpt
                        on fares.order_id = cmpt.order_id
                        Where fare_type = 2
                        and is_current
                        and cmpt.order_date = '{report_date.strftime("%Y-%m-%d")}'
                    """

    # SQL Query for retrieving order location data for completed order_ids
    order_location_sql = f"""SELECT  loc.id
                        , loc.order_id
                        , loc.accept_start_trip_distance
                        FROM {dev_curated_db_name}.{oms_public_schema_name}.order_location_infos loc
                        join {dev_curated_db_name}.{oms_public_schema_name}.completed_spot_orders_fast_mv cmpt
                        on loc.order_id = cmpt.order_id
                        where 1=1
                        and cmpt.order_date = '{report_date.strftime("%Y-%m-%d")}'
                    """

    # Supply sheet
    driver_bs_sql = f"""
                        SELECT
                            *
                        From {dev_curated_db_name}.{oms_public_schema_name}.drivers
                        Where 1=1
                        and active_status = 0
                        and intercity_flag = 0
                        and engagement_type in (0,3) --(mg and trip_based)
                        and app_version >=17  --valid app version
                    """

    driver_login_time_shiftwise_sql = f"""
                                SELECT
                                geo_region_id
                                , driver_id
                                , login_date
                                , start_interval
                                , end_interval
                                , total_login
                                FROM {dev_curated_db_name}.{oms_public_schema_name}.complete_driver_login_ordertime_data_mv
                                Where 1=1
                                and login_date = '{report_date.strftime("%Y-%m-%d")}' 
                                """

    engagement_shift_timings_sql = f"""
                                    SELECT
                                        x.geo_region_id
                                        , x.engagement_start_hour
                                        , x.engagement_start_minutes
                                        , x.engagement_end_hour
                                        , x.engagement_end_minutes
                                    FROM (
                                            SELECT
                                            geo_region_id
                                            , created_at
                                            , engagement_start_hour
                                            , engagement_start_minutes
                                            , engagement_end_hour
                                            , engagement_end_minutes
                                            , row_number() over(partition by geo_region_id order by created_at desc) as rnum
                                            FROM {dev_curated_db_name}.{oms_public_schema_name}.driver_engagement_timings
                                            WHERE engagement_type = 3
                                        ) as x
                                    WHERE x.rnum = 1
                                """

    completed_orders_sql = f"""
                            SELECT
                            order_id
                            from {dev_curated_db_name}.{oms_public_schema_name}.completed_spot_orders_fast_mv
                            Where order_date = '{report_date.strftime("%Y-%m-%d")}'
                            """

    sql_dict = {"query1": order_bs_sql, "query2": geos_sql, "query3": cancel_reason_sql, "query4": customers_first_order_sql, "query5": first_app_order_date_sql,\
       "query6": order_fares_sql, "query7": order_location_sql, "query8": driver_bs_sql, "query9": driver_login_time_shiftwise_sql, "query10": engagement_shift_timings_sql,\
           "query11": completed_orders_sql}
    return sql_dict
