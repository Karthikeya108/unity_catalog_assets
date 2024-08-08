### Row Level Secuirty / Row Filtering with Unity Catalog

https://docs.databricks.com/en/tables/row-and-column-filters.html

#### Example Use Case:

HR Data on Databricks Platform: Filter rows and display only the relevant records based on identity. An individual who is a manager, should be able to view the personal data of his direct reports as well as 3 levels below.

The employee dataset is generated using Python Faker library.

#### How to run the notebook
1. Clone the repo within a Databricks workspace using Git folders (https://docs.databricks.com/en/repos/index.htmls)
2. Execute the commands in the notebook with a UC Shared Cluster or Serverless Compute