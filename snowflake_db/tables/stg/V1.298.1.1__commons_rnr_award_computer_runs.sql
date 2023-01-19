create or replace TABLE STG_DB_INDIA.COMMONS_RNR.AWARD_COMPUTER_RUNS (
	ID NUMBER(38,0) NOT NULL,
	TYPE VARCHAR(16777216) NOT NULL,
	AWARD_COMPUTER_ID NUMBER(38,0) NOT NULL,
	DRIVER_ID NUMBER(38,0) NOT NULL,
	WALLET_AMOUNT NUMBER(38,0),
	FUEL_QUANTITY NUMBER(38,0),
	WALLET_TXN_ID VARCHAR(16777216),
	MULTIPLIER_VALUE NUMBER(38,15),
	START_DATE DATE,
	END_DATE DATE,
	CREATED_AT TIMESTAMP_NTZ(9) NOT NULL,
	UPDATED_AT TIMESTAMP_NTZ(9) NOT NULL,
	DISADVANTAGE_DURATION NUMBER(38,0),
	PRICE NUMBER(38,0),
	VEHICLE_ID NUMBER(38,0),
	INSURANCE_DISCOUNT NUMBER(38,0),
	BENEFIT_TYPE NUMBER(38,0),
	COUPON_ID NUMBER(38,0),
	_ETL_BATCH_ID VARCHAR(16777216),
	_ETL_INSERT_DATE_TIME TIMESTAMP_NTZ(9),
	_ETL_UPDATE_DATE_TIME TIMESTAMP_NTZ(9)
);
