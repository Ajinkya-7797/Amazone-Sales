# Databricks notebook source
# MAGIC %fs ls /FileStore/tables/amzoneAnalysis/

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg,count,col,length,desc,when,sum

# COMMAND ----------

df1 = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load('/FileStore/tables/amzoneAnalysis/amazon.csv')


# COMMAND ----------

df1.printSchema()

# COMMAND ----------

df1.select('category').display()

# COMMAND ----------

Top_rated_products= df1.groupBy('product_id','product_name').agg(avg('rating').alias('avg_rating')).orderBy(desc('avg_rating')).limit(10)
Top_rated_products.show()

# COMMAND ----------

# Most Reviwed Product
most_reviewd_product= df1.groupBy('product_id','product_name').count().orderBy(desc ('count')).limit(10)
most_reviewd_product.show()

# COMMAND ----------

 # discount analysis
 discount = df1.groupBy('category').agg(avg('discount_percentage').alias('avg_discount'))
 discount.display()

# COMMAND ----------

# User Engagement
user_engagement = df1.groupBy('product_id').agg(avg('rating').alias('avg_rating'),count('rating').alias('rating_count')).orderBy(desc('rating_count'))
user_engagement.display()

# COMMAND ----------

 # Creating Temp table from df1
 df1.createOrReplaceTempView('amazon_sales_table')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from amazon_sales_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from amazon_sales_table

# COMMAND ----------


