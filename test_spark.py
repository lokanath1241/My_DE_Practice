from databricks.connect import DatabricksSession
from pyspark.sql.functions import avg, count, sum
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


spark = DatabricksSession.builder.appName("EmployeeSalaryAggregation").getOrCreate()

employee_schema = StructType(
	[
		StructField("employee_id", IntegerType(), nullable=False),
		StructField("employee_name", StringType(), nullable=False),
		StructField("department", StringType(), nullable=False),
		StructField("salary", IntegerType(), nullable=False),
	]
)

employee_data = [
	(1, "Asha", "Engineering", 95000),
	(2, "Ben", "Engineering", 88000),
	(3, "Chloe", "Engineering", 102000),
	(4, "Diego", "Sales", 72000),
	(5, "Emma", "Sales", 78000),
	(6, "Farah", "HR", 68000),
	(7, "George", "HR", 71000),
	(8, "Hana", "Finance", 84000),
]

employees_df = spark.createDataFrame(employee_data, schema=employee_schema)

department_salary_df = (
	employees_df.groupBy("department")
	.agg(
		sum("salary").alias("total_salary"),
		avg("salary").alias("average_salary"),
		count("employee_id").alias("employee_count"),
	)
	.orderBy("department")
)

employees_df.show()
department_salary_df.show()
