USE RETAILS;

SELECT * FROM demographic_RAW;
SELECT * FROM CAMPAIGN_DESC_RAW;
SELECT * FROM CAMPAIGN_RAW;
SELECT * FROM PRODUCT_RAW;
SELECT * FROM COUPON_RAW;
SELECT * FROM COUPON_REDEMPT_RAW;
SELECT * FROM TRANSACTION_RAW;


select * from CAMPAIGN_DESC_NEW;

select * from COUPON_REDEMPT_NEW;

select * from TRANSACTION_NEW;

-----deparment wise product count
SELECT DISTINCT(DEPARTMENT),COUNT(*) AS TOTAL_PRODUCT 
FROM PRODUCT_RAW
GROUP BY 1
ORDER BY 2 DESC;


/*1.	Customer Demographics KPIs:
A. Count of unique households: Measure the total number of unique households in the Demographic table.
B. Household composition distribution: Analyze the distribution of household compositions (HH_COMP_DESC) to understand the composition of households.
C.	Age distribution: Calculate the percentage or count of customers in different age groups (AGE_DESC).
D.	Marital status distribution: Analyze the proportion of customers in different marital status categories (MARITAL_STATUS_CODE).
E.	Income distribution: Determine the distribution of customers across income levels (INCOME_DESC).
F. Homeownership distribution: Calculate the percentage or count of customers who own or rent their homes (HOMEOWNER_DESC).*/

SELECT COUNT(DISTINCT HOUSEHOLD_KEY) AS TOTAL_HOUSEHOLDS FROM DEMOGRAPHIC_RAW; --2,500

SELECT HH_COMP_DESC,COUNT(DISTINCT HOUSEHOLD_KEY) AS TOTAL_HOUSEHOLDS 
FROM DEMOGRAPHIC_RAW
GROUP BY 1
ORDER BY 2 DESC;


SELECT AGE_DESC,TOTAL_HOUSEHOLDS,ROUND(TOTAL_HOUSEHOLDS/2500 * 100,2) AS PERC_AGEWISE_HOUSEHOLDS_DISTR
FROM
(SELECT AGE_DESC,
COUNT(DISTINCT HOUSEHOLD_KEY) AS TOTAL_HOUSEHOLDS
FROM demographic_RAW 
GROUP BY 1
ORDER BY 2 DESC)
GROUP BY 1,2;

SELECT MARITAL_STATUS_CODE , 
COUNT(DISTINCT HOUSEHOLD_KEY) AS TOTAL_HOUSEHOLDS,
ROUND(COUNT(DISTINCT HOUSEHOLD_KEY) / 2500 * 100 , 2) AS PERC_MARITAL_HOUSEHOLDS_DISTR
FROM demographic_RAW
GROUP BY 1
ORDER BY 2 DESC;

SELECT INCOME_DESC , 
COUNT(DISTINCT HOUSEHOLD_KEY) AS TOTAL_HOUSEHOLDS,
ROUND(COUNT(DISTINCT HOUSEHOLD_KEY) / 2500 * 100 , 2) AS PERC_INCOME_HOUSEHOLDS_DISTR
FROM demographic_RAW
GROUP BY 1
ORDER BY 2 DESC;

SELECT HOMEOWNER_DESC , 
COUNT(DISTINCT HOUSEHOLD_KEY) AS TOTAL_HOUSEHOLDS,
ROUND(COUNT(DISTINCT HOUSEHOLD_KEY) / 2500 * 100 , 2) AS PERC_HOMEOWNER_DESC_DISTR
FROM demographic_RAW
GROUP BY 1
ORDER BY 2 DESC;



SELECT T.HOUSEHOLD_KEY,D.AGE_DESC,D.MARITAL_STATUS_CODE,D.INCOME_DESC,AVG(T.SALES_VALUE)AS AVG_AMOUNT,
AVG(T.RETAIL_DISC)AS AVG_RETAIL_DIS,AVG(T.COUPON_DISC)AS AVG_COUPON_DISC,AVG(T.COUPON_MATCH_DISC)AS AVG_COUP_MATCH_DISC
FROM TRANSACTION_NEW T
LEFT OUTER JOIN demographic_RAW D ON T.HOUSEHOLD_KEY =D.HOUSEHOLD_KEY
GROUP BY 1,2,3,4
ORDER BY 1;

--Create PROCEDURE for Household_kpi.
CREATE OR REPLACE PROCEDURE Household_kpi()
RETURNS STRING
LANGUAGE SQL
AS
$$
 CREATE OR REPLACE TABLE Household_kpi AS (SELECT T.HOUSEHOLD_KEY,D.AGE_DESC,D.MARITAL_STATUS_CODE,D.INCOME_DESC,AVG(T.SALES_VALUE)AS AVG_AMOUNT,
AVG(T.RETAIL_DISC)AS AVG_RETAIL_DIS,AVG(T.COUPON_DISC)AS AVG_COUPON_DISC,AVG(T.COUPON_MATCH_DISC)AS AVG_COUP_MATCH_DISC
FROM TRANSACTION_NEW T
LEFT OUTER JOIN demographic_RAW D ON T.HOUSEHOLD_KEY =D.HOUSEHOLD_KEY
GROUP BY 1,2,3,4
ORDER BY 1);
$$;

SHOW PROCEDURES;

CALL Household_kpi();

CREATE OR REPLACE TASK  Household_kpi_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '5 MINUTE' 
AS CALL Household_kpi();

SHOW TASKS;

ALTER TASK   Household_kpi_TASK RESUME;
ALTER TASK  Household_kpi_TASK SUSPEND; 
'''
--Number of campaigns: Count the total number of campaigns in the Campaign table.
-- Campaign duration: Calculate the duration of each campaign by subtracting the start day from the end day (in the Campaign_desc table).
-- Campaign effectiveness: Analyze the number of households associated with each campaign (in the Campaign table) to measure campaign reach.
'''


-- Number of campaigns: Count the total number of campaigns in the Campaign table.
SELECT COUNT( distinct CAMPAIGN) AS TOTAL_CAMPAIGNS FROM CAMPAIGN_DESC_RAW;

-- Campaign duration: Calculate the duration of each campaign by subtracting the start day from the end day (in the CAMPAIGN_DESC_NEW table).
SELECT
  CAMPAIGN,
  DATEDIFF(DAY, START_DATE, END_DATE) AS CAMPAIGN_DURATION
FROM CAMPAIGN_DESC_NEW;

-- Campaign effectiveness: Analyze the number of households associated with each campaign to measure campaign reach.
SELECT CR.CAMPAIGN, COUNT(DISTINCT DR.HOUSEHOLD_KEY) AS TOTAL_HOUSEHOLDS
FROM CAMPAIGN_RAW CR
JOIN DEMOGRAPHIC_RAW DR ON CR.HOUSEHOLD_KEY = DR.HOUSEHOLD_KEY
GROUP BY 1
ORDER BY 2 Desc;

--Create PROCEDURE for Campaign_kpi.
CREATE OR REPLACE PROCEDURE Campaign_kpi()
RETURNS STRING
LANGUAGE SQL
AS
$$
CREATE OR REPLACE TABLE Campaign_kpi AS (
  SELECT
    C.CAMPAIGN,
    D.DESCRIPTION,
    C.HOUSEHOLD_KEY,
    D.START_DATE,
    D.END_DATE,
    D.CAMPAIGN_DURATION,
    D.START_MONTH,
    D.END_MONTH,
    D.START_YEAR,
    D.END_YEAR,
    COUNT(DISTINCT C.HOUSEHOLD_KEY) AS TOTAL_HOUSEHOLDS,
    AVG(D.CAMPAIGN_DURATION) AS AVERAGE_CAMPAIGN_DURATION
  FROM CAMPAIGN_RAW C
  JOIN CAMPAIGN_DESC_NEW D ON C.CAMPAIGN = D.CAMPAIGN
  GROUP BY C.CAMPAIGN, D.DESCRIPTION, C.HOUSEHOLD_KEY, D.START_DATE, D.END_DATE, D.CAMPAIGN_DURATION, D.START_MONTH, D.END_MONTH, D.START_YEAR, D.END_YEAR
);
$$;

SHOW PROCEDURES;

CALL Campaign_kpi();

CREATE OR REPLACE TASK  Campaign_kpi_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '6 MINUTE' 
AS CALL Campaign_kpi();

SHOW TASKS;

ALTER TASK   Campaign_kpi_TASK RESUME;
ALTER TASK  Campaign_kpi_TASK SUSPEND; 





'''
--Coupon redemption rate: Calculate the percentage of coupons redeemed (from the coupon_redempt table) compared to the total number of coupons distributed (from the Coupon table).
-- Coupon usage by campaign: Measure the number of coupon redemptions (from the coupon_redempt table) for each campaign (in the Coupon table).
'''

-- Coupon redemption rate & Coupon usage by campaign
SELECT
  crn.campaign AS campaign_name,
  COUNT(DISTINCT crn.coupon_upc) AS total_coupons,
  COUNT(DISTINCT cr.coupon_upc) AS redeemed_coupons,
  (COUNT(DISTINCT crn.coupon_upc) / COUNT(DISTINCT cr.coupon_upc)) * 100 AS redemption_rate
FROM coupon_redempt_new crn
JOIN coupon_raw cr ON crn.campaign = cr.campaign
GROUP BY 1
ORDER BY 4 Desc;




--Create PROCEDURE for Coupon KPIs.
CREATE OR REPLACE PROCEDURE Coupon_KPI()
RETURNS STRING
LANGUAGE SQL
AS
$$
CREATE OR REPLACE TABLE Coupon_KPI AS (
SELECT
  CRN.HOUSEHOLD_KEY,
  CRN.COUPON_UPC,
  CRN.CAMPAIGN,
  CRN.DATE,
  CRP.PRODUCT_ID
FROM COUPON_REDEMPT_NEW CRN
LEFT JOIN COUPON_RAW CRP ON CRN.CAMPAIGN = CRP.CAMPAIGN
GROUP BY 1,2,3,4
);
$$;

SHOW PROCEDURES;

CALL Coupon_KPI();

CREATE OR REPLACE TASK  Coupon_KPI_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '5 MINUTE' 
AS CALL Coupon_KPI();

SHOW TASKS;

ALTER TASK   Coupon_KPI_TASK RESUME;
ALTER TASK  Coupon_KPI_TASK SUSPEND; 
'''
Sales value: Calculate the total sales value for each product (in the Transaction_data table) to identify top-selling products.
Manufacturer distribution: Analyze the distribution of products across different manufacturers (in the Product table).
Department-wise sales: Measure the sales value by department (in the Product table) to understand which departments contribute most to revenue. Brand-wise sales: Calculate the sales value for each brand (in the Product table) to
identify top-selling brands.
'''

-- Sales value: Calculate the total sales value for each product (in the Transaction_data table) to identify top-selling products.
CREATE OR REPLACE TABLE top_selling_products AS
SELECT
  P.COMMODITY_DESC,
  P.SUB_COMMODITY_DESC,
  SUM(T.SALES_VALUE) AS TOTAL_SALES_VALUE
FROM TRANSACTION_RAW T
JOIN PRODUCT_RAW P ON T.PRODUCT_ID = P.PRODUCT_ID
GROUP BY  1,2
ORDER BY 3 DESC;

select * from top_selling_products;

--Manufacturer distribution: Analyze the distribution of products across different manufacturers (in the Product table).
CREATE OR REPLACE TABLE manufacturers_productsCount AS
SELECT
  MANUFACTURER,
  COUNT(*) AS PRODUCT_COUNT
FROM PRODUCT_RAW
GROUP BY 1
ORDER BY 1 DESC;

select * from manufacturers_productsCount;

-- Department-wise sales: Measure the sales value by department (in the Product table) to understand which departments contribute most to revenue.
CREATE OR REPLACE TABLE Department_wise_sales AS
SELECT
  DEPARTMENT,
  SUM(T.SALES_VALUE) AS TOTAL_SALES_VALUE
FROM TRANSACTION_RAW T
JOIN PRODUCT_RAW P ON T.PRODUCT_ID = P.PRODUCT_ID
GROUP BY DEPARTMENT
ORDER BY TOTAL_SALES_VALUE DESC;

-- Brand-wise sales: Calculate the sales value for each brand (in the Product table) to identify top-selling brands.
CREATE OR REPLACE TABLE Brand_wise_sales AS
SELECT
  BRAND,
  SUM(T.SALES_VALUE) AS TOTAL_SALES_VALUE
FROM TRANSACTION_RAW T
JOIN PRODUCT_RAW P ON T.PRODUCT_ID = P.PRODUCT_ID
GROUP BY BRAND
ORDER BY TOTAL_SALES_VALUE DESC;

CREATE OR REPLACE PROCEDURE Product_kpi()
RETURNS STRING
LANGUAGE SQL
AS
$$
CREATE OR REPLACE TABLE top_selling_products AS
SELECT
  P.COMMODITY_DESC,
  P.SUB_COMMODITY_DESC,
  SUM(T.SALES_VALUE) AS TOTAL_SALES_VALUE
FROM TRANSACTION_RAW T
JOIN PRODUCT_RAW P ON T.PRODUCT_ID = P.PRODUCT_ID
GROUP BY  1, 2
ORDER BY 3 DESC;

CREATE OR REPLACE TABLE manufacturers_productsCount AS
SELECT
  MANUFACTURER,
  COUNT(*) AS PRODUCT_COUNT
FROM PRODUCT_RAW
GROUP BY 1
ORDER BY 1 DESC;

CREATE OR REPLACE TABLE Department_wise_sales AS
SELECT
  DEPARTMENT,
  SUM(T.SALES_VALUE) AS TOTAL_SALES_VALUE
FROM TRANSACTION_RAW T
JOIN PRODUCT_RAW P ON T.PRODUCT_ID = P.PRODUCT_ID
GROUP BY DEPARTMENT
ORDER BY TOTAL_SALES_VALUE DESC;

CREATE OR REPLACE TABLE Brand_wise_sales AS
SELECT
  BRAND,
  SUM(T.SALES_VALUE) AS TOTAL_SALES_VALUE
FROM TRANSACTION_RAW T
JOIN PRODUCT_RAW P ON T.PRODUCT_ID = P.PRODUCT_ID
GROUP BY BRAND
ORDER BY TOTAL_SALES_VALUE DESC;
$$;

SHOW PROCEDURES;
