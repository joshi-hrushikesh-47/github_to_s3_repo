create or replace TABLE STG_DB_INDIA.REDSHIFT_AWSMA.DRIVER_APP_EVENTS (
	EVENT_TYPE VARCHAR(16777216) NOT NULL,
	EVENT_TIMESTAMP TIMESTAMP_NTZ(9) NOT NULL,
	ARRIVAL_TIMESTAMP TIMESTAMP_NTZ(9) NOT NULL,
	EVENT_VERSION VARCHAR(16777216) NOT NULL,
	APPLICATION_VERSION_NAME VARCHAR(16777216),
	APPLICATION_VERSION_CODE VARCHAR(16777216),
	CLIENT_ID VARCHAR(16777216) NOT NULL,
	CLIENT_COGNITO_ID VARCHAR(16777216),
	DEVICE_MODEL VARCHAR(16777216),
	DEVICE_MAKE VARCHAR(16777216),
	DEVICE_PLATFORM_NAME VARCHAR(16777216),
	DEVICE_PLATFORM_VERSION VARCHAR(16777216),
	SESSION_ID VARCHAR(16777216),
	SESSION_START_TIMESTAMP TIMESTAMP_NTZ(9),
	SESSION_STOP_TIMESTAMP TIMESTAMP_NTZ(9),
	DRIVER_ID NUMBER(38,0) NOT NULL,
	UPLOAD_TAG VARCHAR(16777216) NOT NULL,
	A_MSISDN VARCHAR(16777216),
	A_ORDER_ID VARCHAR(16777216),
	A_SESSION_ID VARCHAR(16777216),
	A_ORDER_SESSION_ID VARCHAR(16777216),
	A_STATE VARCHAR(16777216),
	A_TAG VARCHAR(16777216),
	A_TIMESTAMP VARCHAR(16777216),
	A_TIME VARCHAR(16777216),
	A_TIME_SPENT VARCHAR(16777216),
	A_GUIDE_ID VARCHAR(16777216),
	A_SLIDE_ID VARCHAR(16777216),
	A_BUTTON VARCHAR(16777216),
	A_AMAZON_ID VARCHAR(16777216),
	A_BUCKET VARCHAR(16777216),
	A_KEY VARCHAR(16777216),
	A_PROGRESS_EVENT VARCHAR(16777216),
	A_PROGRESS VARCHAR(16777216),
	A_ERROR_MESSAGE VARCHAR(16777216),
	A_URI VARCHAR(16777216),
	A_METHOD VARCHAR(16777216),
	A_ERROR_CODE VARCHAR(16777216),
	A_RECEIVER VARCHAR(16777216),
	A_ACTION VARCHAR(16777216),
	A_TYPE VARCHAR(16777216),
	A_LOCATION VARCHAR(16777216),
	A_REQUEST_CODE VARCHAR(16777216),
	A_RESULT_CODE VARCHAR(16777216),
	A_REASON VARCHAR(16777216),
	A_HAS_RESOLUTION VARCHAR(16777216),
	A_SHOWING_LAYOUT VARCHAR(16777216),
	A_ORDER_DETAILS VARCHAR(16777216),
	A_CHECK VARCHAR(16777216),
	A_CYCLE_TYPE VARCHAR(16777216),
	A_PERIOD_STR VARCHAR(16777216),
	A_CACHE_KEY VARCHAR(16777216),
	A_CACHE_STATUS VARCHAR(16777216),
	A_CACHE_EVENT VARCHAR(16777216),
	A_JOB_TAG VARCHAR(16777216),
	A_ALLOWED VARCHAR(16777216),
	A_SLIDE_RANK VARCHAR(16777216),
	A_INITED VARCHAR(16777216),
	A_ERROR_STACKTRACE VARCHAR(16777216),
	A_DESC VARCHAR(16777216),
	A_GRANTED VARCHAR(16777216),
	A_RETRY_REQUEST VARCHAR(16777216),
	A_REF VARCHAR(16777216),
	M_PAYLOAD_SIZE NUMBER(18,0),
	M_TIME_TAKEN NUMBER(18,0),
	A_IS_ONLINE VARCHAR(16777216),
	A_NEW_ORDER_ID VARCHAR(16777216),
	A_RETRY_COUNT NUMBER(38,0),
	A_SOURCE VARCHAR(16777216),
	A_ZOMBIE_IDENTIFIER VARCHAR(16777216),
	A_SUCCESS VARCHAR(16777216),
	A_CONNECTIVITY_MANAGER_PRESENT VARCHAR(16777216),
	A_NETWORK_INFO_PRESENT VARCHAR(16777216),
	A_IS_NETWORK_CONNECTED VARCHAR(16777216),
	A_NETWORK_TYPE VARCHAR(16777216),
	_ETL_BATCH_ID VARCHAR(16777216),
	_ETL_INSERT_DATE_TIME TIMESTAMP_NTZ(9),
	_ETL_UPDATE_DATE_TIME TIMESTAMP_NTZ(9)
);
