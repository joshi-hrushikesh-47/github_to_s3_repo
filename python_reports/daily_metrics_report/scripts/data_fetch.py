# import psycopg2

# from constants import credentials, report_date, start_time, end_time
# from user_functions import read_sql_tmpfile


def fetch_data(sql_dict, session):
    # calling function for retrieving data. This triggers sql queries

    # Demand Sheet Queries
    orders_base_raw = session.sql(list(sql_dict.values())[0])
    orders_base_raw = orders_base_raw.to_pandas()

    geos_base_raw = session.sql(list(sql_dict.values())[1])
    geos_base_raw = geos_base_raw.to_pandas()

    cancel_reason_base = session.sql(list(sql_dict.values())[2])
    cancel_reason_base = cancel_reason_base.to_pandas()

    customers_first_order_base = session.sql(list(sql_dict.values())[3])
    customers_first_order_base = customers_first_order_base.to_pandas()

    first_app_order_date_base = session.sql(list(sql_dict.values())[4])
    first_app_order_date_base = first_app_order_date_base.to_pandas()

    completed_base_raw = session.sql(list(sql_dict.values())[10])
    completed_base_raw = completed_base_raw.to_pandas()

    order_fares_base = session.sql(list(sql_dict.values())[5])
    order_fares_base = order_fares_base.to_pandas()

    order_location_base = session.sql(list(sql_dict.values())[6])
    order_location_base = order_location_base.to_pandas()

    # Supply sheet queries
    drivers_base_raw = session.sql(list(sql_dict.values())[7])
    drivers_base_raw = drivers_base_raw.to_pandas()

    login_time_base_raw = session.sql(list(sql_dict.values())[8])
    login_time_base_raw = login_time_base_raw.to_pandas()

    engagement_shift_timings_base_raw = session.sql(list(sql_dict.values())[9])
    engagement_shift_timings_base_raw = engagement_shift_timings_base_raw.to_pandas()

    raw_bases = {"base1": orders_base_raw, "base2": geos_base_raw, "base3": cancel_reason_base, "base4": customers_first_order_base,\
        "base5": first_app_order_date_base, "base6": order_fares_base, "base7": order_location_base, "base8": drivers_base_raw,\
        "base9": login_time_base_raw, "base10": engagement_shift_timings_base_raw, "base11": completed_base_raw}
    return raw_bases



# def fetch_data_old(sql_dict, session):
#     # calling function for retrieving data. This triggers sql queries
#
#     # Demand Sheet Queries
#     orders_base_raw = read_sql_tmpfile(list(sql_dict.values())[0], 'oms_snapshot_db')  # 1
#
#     geos_base_raw = read_sql_tmpfile(list(sql_dict.values())[1], 'oms_snapshot_db')  # 2
#
#     cancel_reason_base = read_sql_tmpfile(list(sql_dict.values())[2], 'oms_snapshot_db')  # 3
#
#     customers_first_order_base = read_sql_tmpfile(list(sql_dict.values())[3], 'oms_snapshot_db')  # 4
#
#     first_app_order_date_base = read_sql_tmpfile(list(sql_dict.values())[4], 'oms_snapshot_db')  # 5
#
#     completed_base_raw = read_sql_tmpfile(list(sql_dict.values())[10], 'oms_snapshot_db')  # 6
#
#     # list of completed order ids on report date
#     completed_order_ids = completed_base_raw['order_id'].to_list()  # 7
#
#     # order_fares_sql query
#     # modifying query for binding completed order_id list in the sql query
#     db_credentials = credentials['databases']['oms_snapshot_db']
#     conn = psycopg2.connect(**db_credentials)
#     cur = conn.cursor()
#
#     modified_order_fares_sql = str(cur.mogrify(list(sql_dict.values())[5], (completed_order_ids, )), 'utf-8')
#
#     # Retrieving order fare data for completed order_ids
#     order_fares_base = read_sql_tmpfile(modified_order_fares_sql, 'oms_snapshot_db')
#
#     # order_location_sql query
#     # modifying query for binding completed order_id list in the sql query
#     modified_order_location_sql = str(cur.mogrify(list(sql_dict.values())[6], (completed_order_ids, )), 'utf-8')
#
#     # Retrieving order location data for completed order_ids
#     order_location_base = read_sql_tmpfile(modified_order_location_sql, 'oms_snapshot_db')  # 8
#
#     # Supply sheet queries
#     drivers_base_raw = read_sql_tmpfile(list(sql_dict.values())[7], 'oms_snapshot_db')  # 9
#
#     login_time_base_raw = read_sql_tmpfile(list(sql_dict.values())[8], 'oms_snapshot_db')  # 10
#
#     engagement_shift_timings_base_raw = read_sql_tmpfile(list(sql_dict.values())[9], 'oms_snapshot_db')  # 11
#
#     raw_bases = {"base1": orders_base_raw, "base2": geos_base_raw, "base3": cancel_reason_base, "base4": customers_first_order_base,\
#         "base5": first_app_order_date_base, "base6": order_fares_base, "base7": order_location_base, "base8": drivers_base_raw,\
#         "base9": login_time_base_raw, "base10": engagement_shift_timings_base_raw, "base11": completed_base_raw}
#     return raw_bases
