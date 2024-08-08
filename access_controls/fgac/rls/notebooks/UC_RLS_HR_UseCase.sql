-- Databricks notebook source
USE CATALOG main;
USE SCHEMA default;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #read the files directly from the repo
-- MAGIC """
-- MAGIC employees_df = spark.read.option("delimiter", ";").csv("file:/Workspace/Users/[USERNAME]/unity_catalog_assets/access_controls/fgac/rls/data/employees.csv", header=True)
-- MAGIC managers_df = spark.read.option("delimiter", ";").csv("file:/Workspace/Users/[USERNAME]/unity_catalog_assets/access_controls/fgac/rls/data/managers.csv", header=True)
-- MAGIC """
-- MAGIC #OR store the data files in a UC Volume and read it from there
-- MAGIC employees_df = spark.read.option("delimiter", ";").csv("/Volumes/main/default/data/employees.csv", header=True)
-- MAGIC managers_df = spark.read.option("delimiter", ";").csv("/Volumes/main/default/data/managers.csv", header=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(employees_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(managers_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC employees_df.write.mode("overwrite").saveAsTable("employees")
-- MAGIC managers_df.write.mode("overwrite").saveAsTable("managers")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Employee Hierarchy
-- MAGIC
-- MAGIC <img src="https://github.com/Karthikeya108/unity_catalog_assets/raw/main/access_controls/fgac/rls/images/hierarchy.png" width="800">

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
  RETURN (manager = CURRENT_USER());

-- COMMAND ----------

ALTER TABLE employees SET ROW FILTER row_filter_direct_reports ON (manager);

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
  RETURN (email = CURRENT_USER())
  OR (email IN (SELECT emml.employee_email
 FROM main.default.employee_manager_mulitple_levels emml
    WHERE emml.manager_level = CURRENT_USER()));

-- COMMAND ----------

ALTER TABLE employees SET ROW FILTER row_filter_reports ON (email);

-- COMMAND ----------

SELECT * FROM employees ORDER BY manager;

-- COMMAND ----------

-- MAGIC %environment
-- MAGIC "client": "1"
-- MAGIC "base_environment": ""
