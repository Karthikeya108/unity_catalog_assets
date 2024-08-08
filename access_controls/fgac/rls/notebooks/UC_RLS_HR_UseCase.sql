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
-- MAGIC employees_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("employees")
-- MAGIC managers_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("managers")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Employee Hierarchy
-- MAGIC
-- MAGIC <img src="https://github.com/Karthikeya108/unity_catalog_assets/raw/main/access_controls/fgac/rls/images/hierarchy.png" width="800">

-- COMMAND ----------

-- MAGIC %python
-- MAGIC """
-- MAGIC In order to make it easier to implement the Row Filtering logic, I am adding another column `manager` (that stores the email address) to the `employees` table
-- MAGIC """
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
SELECT * FROM employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Scenario 1: Show only Direct Reports
-- MAGIC In the first scenario, I will create a row filter finction `row_filter_direct_reports` with `email` and `manager` as parameters. This function when applied to the `employees` table will return rows that match employee who is querying the data as well as the data of thier direct report.

-- COMMAND ----------

--If the row filter refers to one or more tables, you cannot apply the row filter to any of those tables.
CREATE OR REPLACE FUNCTION row_filter_direct_reports(email STRING, manager STRING)
RETURN (email = CURRENT_USER())
OR (manager = CURRENT_USER());

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For testing purposes, try replace the `CURRENT_USER()` string in the above function with one of the email ids from the employees table (eg., `Danielle.Gibbs@company.com`)

-- COMMAND ----------

ALTER TABLE employees SET ROW FILTER row_filter_direct_reports ON (email, manager);

-- COMMAND ----------

SELECT * FROM employees ORDER BY manager;

-- COMMAND ----------

--Clean up: Drop the filter from the table
ALTER TABLE employees DROP ROW FILTER;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Scenario 2: Show reporting employees upto 3 levels below
-- MAGIC In the second scenario, I will create a row filter finction `row_filter_reports` with `email` as a parameter. This function when applied to the `employees` table will return rows that match employee who is querying the data as well as the data of the reporting employees upto 3 levels below.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC I am creating a table `reporting_levels` to capture the manager<>employee hierarchial structure. I will use this table to create yet another table `employee_manager_mulitple_levels` to simply the implementation of the row filter function.

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

-- MAGIC %md
-- MAGIC For testing purposes, try replace the `CURRENT_USER()` string in the above function with one of the email ids from the employees table (eg., `Jessica.Carey@company.com`)

-- COMMAND ----------

ALTER TABLE employees SET ROW FILTER row_filter_reports ON (email);

-- COMMAND ----------

SELECT * FROM employees ORDER BY manager;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC If you replace the `CURRENT_USER()` function with `Jessica.Carey@company.com` for testing, then you will notice that it returns all the records except for `Marissa Campbell` who is 4 levels below `Jessica Carey` (as expected)

-- COMMAND ----------

--Clean up: Drop the filter from the table
ALTER TABLE employees DROP ROW FILTER;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Notes:
-- MAGIC - I haven't done any performance testing for this code. So, I am not sure if this scales.
-- MAGIC - The row filter function is placed under the catalog and schema and hence UC access controls are applicable of these functions as well.
-- MAGIC
