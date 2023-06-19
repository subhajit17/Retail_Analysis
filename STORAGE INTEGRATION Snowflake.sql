--AWS (S3) INTEGRATION------------------------------------------------------------------------
USE RETAILS;

CREATE OR REPLACE STORAGE integration s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN ='arn:aws:iam::961695300193:role/retailrole' 
STORAGE_ALLOWED_LOCATIONS =('s3://retailrww/');

DESC integration s3_int;


CREATE OR REPLACE STAGE RETAIL
URL ='s3://retailrww'
file_format = CSV
storage_integration = s3_int;

LIST @RETAIL;

SHOW STAGES;