-- Databricks notebook source
USE CATALOG main;
USE SCHEMA default;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %pip install -q pandas faker mimesis
-- MAGIC dbutils.library.restartPython()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC from typing import Iterator
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *
-- MAGIC import time
-- MAGIC from datetime import date
-- MAGIC import random
-- MAGIC from faker import Faker
-- MAGIC from mimesis import Generic
-- MAGIC from mimesis.locales import Locale

-- COMMAND ----------

-- MAGIC %python
-- MAGIC @pandas_udf("long")
-- MAGIC def get_id(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
-- MAGIC   for id in batch_iter:
-- MAGIC       yield int(time.time()) + id

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC employee_schema = StructType([
-- MAGIC   StructField("employee_id", LongType(), False),
-- MAGIC   StructField("name", StringType(), False),
-- MAGIC   StructField("email", StringType(), False),
-- MAGIC   StructField("gender", StringType(), False),
-- MAGIC   StructField("date_of_birth", DateType(), False),
-- MAGIC   StructField("age", LongType(), False),
-- MAGIC   StructField("address", StringType(), False),
-- MAGIC   StructField("phone_number", StringType(), False),
-- MAGIC   StructField("ssn", StringType(), False),
-- MAGIC   StructField("salary", LongType(), False),
-- MAGIC   StructField("iban", StringType(), False),
-- MAGIC   ])
-- MAGIC
-- MAGIC def generate_fake_employee_data(pdf: pd.DataFrame) -> pd.DataFrame:
-- MAGIC
-- MAGIC   fake = Faker('en_US')
-- MAGIC   generic = Generic(locale=Locale.EN)
-- MAGIC   domain_name = "company.com"
-- MAGIC     
-- MAGIC   def generate_data(y):
-- MAGIC     
-- MAGIC     dob = fake.date_between(start_date='-60y', end_date='-18y')
-- MAGIC
-- MAGIC     y["gender"] = fake.passport_gender()
-- MAGIC     y["name"] = fake.name()
-- MAGIC     y["email"] = fake.email()
-- MAGIC     first_name = fake.first_name_male() if y["gender"] =="M" else fake.first_name_female()
-- MAGIC     last_name = fake.last_name()
-- MAGIC     y["name"] = f"{first_name} {last_name}"
-- MAGIC     y["email"] = f"{first_name}.{last_name}@{domain_name}"
-- MAGIC     y["date_of_birth"] = dob #.strftime("%Y-%m-%d")
-- MAGIC     y["age"] = date.today().year - dob.year
-- MAGIC     y["address"] = fake.address()
-- MAGIC     y["phone_number"] = fake.phone_number()
-- MAGIC     y["ssn"] = fake.ssn()
-- MAGIC     y["salary"] = fake.random_int(min=65000, max=900000)
-- MAGIC     y["iban"] = fake.iban()
-- MAGIC
-- MAGIC     return y
-- MAGIC     
-- MAGIC   return pdf.apply(generate_data, axis=1).drop(["partition_id", "id"], axis=1)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC num_rows = 10

-- COMMAND ----------

-- MAGIC %python
-- MAGIC initial_data = spark.range(1, num_rows+1).withColumn("employee_id", get_id(col("id")))
-- MAGIC employees_df = (initial_data
-- MAGIC   .withColumn("partition_id", spark_partition_id())
-- MAGIC   .groupBy("partition_id")
-- MAGIC   .applyInPandas(generate_fake_employee_data, employee_schema)
-- MAGIC   .orderBy(asc("employee_id")))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC employees_df.write.mode("overwrite").saveAsTable("employees")

-- COMMAND ----------

SELECT * FROM employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Employee Hierarchy
-- MAGIC
-- MAGIC <img src="https://github.com/Karthikeya108/unity_catalog_assets/raw/main/access_controls/fgac/rls/images/hierarchy.png" width="800">

-- COMMAND ----------

CREATE OR REPLACE TABLE managers
  (employee_id LONG, employee_email STRING, direct_report LONG);

INSERT INTO managers
VALUES
  (1723035680, 'Jessica.Carey@company.com', 1723035681),
  (1723035680, 'Jessica.Carey@company.com', 1723035679),
  (1723035680, 'Jessica.Carey@company.com', 1723035686),
  (1723035681, 'Kayla.Johnson@company.com', 1723035684),
  (1723035681, 'Kayla.Johnson@company.com', 1723035677),
  (1723035679, 'Jessica.Robinson@company.com', 1723035685),
  (1723035686, 'Martha.Weiss@company.com', 1723035682),
  (1723035685, 'Lisa.Sanders@company.com', 1723035683),
  (1723035683, 'Danielle.Gibbs@company.com', 1723035678);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC emp_df =  spark.read.table("employees")
-- MAGIC man_df =  spark.read.table("managers")
-- MAGIC
-- MAGIC # Perform the join and select the necessary columns including a new 'manager_id' column
-- MAGIC emp_df = emp_df.join(
-- MAGIC     man_df,
-- MAGIC     emp_df['employee_id'] == man_df['direct_report'],
-- MAGIC     'leftouter'
-- MAGIC ).select(
-- MAGIC     emp_df["*"],  # Select all columns from emp_df
-- MAGIC     man_df.employee_email.alias('manager')  # Create a new column 'manager_id' from man_df.employee_id
-- MAGIC )
-- MAGIC
-- MAGIC emp_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("employees")

-- COMMAND ----------

--'manager' column in the employee table needs to be up to date
-- multiple skip levels upto 4 needs to be added
SELECT * FROM employees;

-- COMMAND ----------

DROP FUNCTION IF EXISTS row_filter_direct_reports;

--If the row filter refers to one or more tables, you cannot apply the row filter to any of those tables.
CREATE FUNCTION row_filter_direct_reports(manager STRING)
RETURNS BOOLEAN
  --RETURN (manager = CURRENT_USER());
  RETURN 1

-- COMMAND ----------

ALTER TABLE employees SET ROW FILTER row_filter_direct_reports ON (manager);

-- COMMAND ----------

SELECT CURRENT_USER();

-- COMMAND ----------

SELECT * FROM employees ORDER BY manager;

-- COMMAND ----------

CREATE OR REPLACE TABLE reporting_levels
AS SELECT t1.name as level_0_name, 
          t1.email as level_0_email,
          t2.name as level_1_name, 
          t2.email as level_1_email,
          t3.name as level_2_name, 
          t3.email as level_2_email,
          t4.name as level_3_name, 
          t4.email as level_3_email
FROM employees t1
    LEFT JOIN employees t2 ON t2.email = t1.manager
    LEFT JOIN employees t3 ON t3.email = t2.manager
    LEFT JOIN employees t4 ON t4.email = t3.manager;

-- COMMAND ----------

SELECT * FROM reporting_levels;

-- COMMAND ----------

CREATE OR REPLACE TABLE employee_manager_mulitple_levels
AS SELECT level_0_email AS employee_email, level_0_name AS employee_name, Value AS manager_level
  FROM reporting_levels
  UNPIVOT (
    Value
    FOR NewCol IN (level_1_email, level_2_email, level_3_email)
  ) ORDER BY Value

-- COMMAND ----------

SELECT * FROM employee_manager_mulitple_levels ORDER BY manager_level;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION row_filter_reports(email STRING)
  --RETURN (email = CURRENT_USER()) 
  RETURN TRUE
  OR (email IN (SELECT emml.employee_email
 FROM main.default.employee_manager_mulitple_levels emml
    WHERE emml.manager_level = CURRENT_USER()));

-- COMMAND ----------

ALTER TABLE employees SET ROW FILTER row_filter_reports ON (email);

-- COMMAND ----------

SELECT * FROM employees ORDER BY manager;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %environment
-- MAGIC "client": "1"
-- MAGIC "base_environment": ""
