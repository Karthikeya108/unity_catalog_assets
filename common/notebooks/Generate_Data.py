# Databricks notebook source
# MAGIC %pip install -q pandas faker mimesis
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
from typing import Iterator
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
from datetime import date
import random
from faker import Faker
from mimesis import Generic
from mimesis.locales import Locale

# COMMAND ----------

@pandas_udf("long")
def get_id(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
  for id in batch_iter:
      yield int(time.time()) + id

# COMMAND ----------

employee_schema = StructType([
  StructField("employee_id", LongType(), False),
  StructField("name", StringType(), False),
  StructField("email", StringType(), False),
  StructField("gender", StringType(), False),
  StructField("date_of_birth", DateType(), False),
  StructField("age", LongType(), False),
  StructField("address", StringType(), False),
  StructField("phone_number", StringType(), False),
  StructField("ssn", StringType(), False),
  StructField("salary", LongType(), False),
  StructField("iban", StringType(), False),
  ])

def generate_fake_employee_data(pdf: pd.DataFrame) -> pd.DataFrame:

  fake = Faker('en_US')
  generic = Generic(locale=Locale.EN)
  domain_name = "company.com"
    
  def generate_data(y):
    
    dob = fake.date_between(start_date='-60y', end_date='-18y')

    y["gender"] = fake.passport_gender()
    y["name"] = fake.name()
    y["email"] = fake.email()
    first_name = fake.first_name_male() if y["gender"] =="M" else fake.first_name_female()
    last_name = fake.last_name()
    y["name"] = f"{first_name} {last_name}"
    y["email"] = f"{first_name}.{last_name}@{domain_name}"
    y["date_of_birth"] = dob #.strftime("%Y-%m-%d")
    y["age"] = date.today().year - dob.year
    y["address"] = fake.address()
    y["phone_number"] = fake.phone_number()
    y["ssn"] = fake.ssn()
    y["salary"] = fake.random_int(min=65000, max=900000)
    y["iban"] = fake.iban()

    return y
    
  return pdf.apply(generate_data, axis=1).drop(["partition_id", "id"], axis=1)

# COMMAND ----------

num_rows = 1000

# COMMAND ----------

#generate the dataset
initial_data = spark.range(1, num_rows+1).withColumn("employee_id", get_id(col("id")))
employees_df = (initial_data
  .withColumn("partition_id", spark_partition_id())
  .groupBy("partition_id")
  .applyInPandas(generate_fake_employee_data, employee_schema)
  .orderBy(asc("employee_id")))

# COMMAND ----------

#persist the data
employees_df.write.mode("overwrite").saveAsTable("main.default.employees")

# COMMAND ----------

#export data to CSV files
employees_df = employees_df.toPandas().replace(r'\n',  ' ', regex=True)
employees_df.to_csv("/Volumes/main/default/data/employees.csv", sep=";", index=False)
