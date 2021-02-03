create table CSE532.DEA_NY(REPORTER_DEA_NO varchar(100),REPORTER_BUS_ACT varchar(100), REPORTER_NAME varchar(200), REPORTER_ADDL_CO_INFO varchar(200), REPORTER_ADDRESS1 varchar(300), REPORTER_ADDRESS2 varchar(300), REPORTER_CITY varchar(200), REPORTER_STATE varchar(200),REPORTER_ZIP varchar(200),REPORTER_COUNTRY varchar(200),BUYER_DEA_NO varchar(100),BUYER_BUS_ACT varchar(100), BUYER_NAME varchar(200), BUYER_ADDL_CO_INFO varchar(200), BUYER_ADDRESS1 varchar(300), BUYER_ADDRESS2 varchar(300), BUYER_CITY varchar(200), BUYER_STATE varchar(200),BUYER_ZIP varchar(200),BUYER_COUNTRY varchar(200),TRANSACTION_CODE varchar(50), DRUG_CODE varchar(10) , NDC_NO varchar(50), DRUG_NAME varchar(200), QUANTITY DOUBLE,UNIT varchar(100),ACTION_INDICATOR varchar(100), ORDER_FORM_NO varchar(100), CORRECTION_NO varchar(100), STRENGTH varchar(100), TRANSACTION_DATE DATE, CALC_BASE_WT_IN_GM DOUBLE, DOSAGE_UNIT DOUBLE, TRANSACTION_ID varchar(100), Product_Name varchar(100), Ingredient_Name varchar(100), Measure varchar(100), MME_Conversion_Factor DOUBLE, Combined_Labeler_Name varchar(100), Reporter_family varchar(100), dos_str varchar(20), MME DOUBLE ) COMPRESS YES;