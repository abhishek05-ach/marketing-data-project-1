# Databricks notebook source
df_csv = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/workspace/default/marketing_volume/marketing.csv")

# COMMAND ----------

df_json = spark.read.format("json") \
    .load("/Volumes/workspace/default/marketing_volume/campaign.json")

# COMMAND ----------

df_csv.columns
df_json.columns

# COMMAND ----------

df_json = df_json.withColumnRenamed("id", "campaign_id")

# COMMAND ----------

df_final = df_csv.join(df_json, "campaign_id", "left")

# COMMAND ----------

from pyspark.sql.functions import expr

df_final = df_final.withColumn("CTR", expr("clicks / impressions")) \
                   .withColumn("conversion_rate", expr("orders / clicks"))

df_final.groupBy("channel").sum("clicks","impressions","orders").show()

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/default/marketing_volume/"))

# COMMAND ----------

df_csv = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/workspace/default/marketing_volume/marketing.csv")

# COMMAND ----------

df_json = spark.read.format("json") \
    .load("/Volumes/workspace/default/marketing_volume/marketing-campaigns.json")

# COMMAND ----------

df_csv.columns
df_json.columns

# COMMAND ----------

df_json = df_json.withColumnRenamed("id", "campaign_id")

# COMMAND ----------

df_final = df_csv.join(df_json, "campaign_id", "left")

# COMMAND ----------

from pyspark.sql.functions import expr

df_final = df_final.withColumn("CTR", expr("clicks / impressions")) \
                   .withColumn("conversion_rate", expr("orders / clicks"))

df_final.groupBy("channel") \
    .sum("clicks","impressions","orders") \
    .show()

# COMMAND ----------

df_json = spark.read.format("json") \
    .option("multiline", "true") \
    .load("/Volumes/workspace/default/marketing_volume/marketing-campaigns.json")

# COMMAND ----------

df_csv = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/workspace/default/marketing_volume/marketing.csv")

# COMMAND ----------

df_csv = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/workspace/default/marketing_volume/marketing.csv")

# COMMAND ----------

df_json = df_json.withColumnRenamed("id", "campaign_id")

# COMMAND ----------

df_final = df_csv.join(df_json, "campaign_id", "left")

# COMMAND ----------

df_final.show(5)

# COMMAND ----------

df_final = df_csv.join(df_json, ["campaign_name", "channel"], "left")

# COMMAND ----------

from pyspark.sql.functions import expr

df_final = df_final.withColumn("CTR", expr("clicks/impressions"))

df_final.groupBy("channel").sum("clicks","impressions","revenue").show()

# COMMAND ----------

df_final = df_csv.join(df_json, "campaign_name", "left")

# COMMAND ----------

df_final = df_final.dropDuplicates()

# COMMAND ----------

from pyspark.sql.functions import expr

df_final = df_final.withColumn("CTR", expr("clicks/impressions")) \
                   .withColumn("conversion_rate_calc", expr("orders/clicks"))

df_final.groupBy("campaign_name") \
    .sum("revenue","mark_spent","clicks") \
    .show()

# COMMAND ----------

df_csv = df_csv.withColumnRenamed("revenue", "csv_revenue")
df_json = df_json.withColumnRenamed("revenue", "json_revenue")

# COMMAND ----------

df_final = df_csv.join(df_json, "campaign_name", "left")

# COMMAND ----------

df_final = df_csv.join(df_json, "campaign_name", "left") \
    .select(
        df_csv["*"],
        df_json["channel"],
        df_json["budget"],
        df_json["roi"]
    )

# COMMAND ----------

df_final = df_csv.join(df_json, "campaign_name", "left") \
    .select(
        df_csv["*"],
        df_json["channel"],
        df_json["budget"],
        df_json["roi"]
    )

# COMMAND ----------

df_final.groupBy("campaign_name") \
    .sum("csv_revenue", "mark_spent") \
    .show()

# COMMAND ----------

from pyspark.sql.functions import expr

df_final = df_final.withColumn("CTR", expr("clicks / impressions")) \
                   .withColumn("conversion_rate_calc", expr("orders / clicks")) \
                   .withColumn("CPC", expr("mark_spent / clicks")) \
                   .withColumn("ROI_calc", expr("csv_revenue / mark_spent"))

# COMMAND ----------

df_final.groupBy("channel") \
    .sum("clicks", "impressions", "orders", "csv_revenue", "mark_spent") \
    .show()

# COMMAND ----------

df_final.orderBy("csv_revenue", ascending=False).show()

# COMMAND ----------

df_final.orderBy("ROI_calc").show()

# COMMAND ----------

df_final.select("campaign_name", "conversion_rate_calc") \
    .orderBy("conversion_rate_calc", ascending=False) \
    .show()

# COMMAND ----------

df_final.select("campaign_name", "budget", "csv_revenue", "ROI_calc") \
    .orderBy("ROI_calc", ascending=False) \
    .show()

# COMMAND ----------

df_final.groupBy("c_date") \
    .sum("clicks", "orders", "csv_revenue") \
    .show()

# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("marketing_final")

# COMMAND ----------

spark.sql("SHOW TABLES").show()

# COMMAND ----------

df = spark.table("marketing_final")
df.show(5)

# COMMAND ----------

df = spark.table("marketing_final")
df.show(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT channel, SUM(csv_revenue) AS total_revenue
# MAGIC FROM marketing_final
# MAGIC GROUP BY channel
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT campaign_name, SUM(csv_revenue) AS revenue
# MAGIC FROM marketing_final
# MAGIC GROUP BY campaign_name
# MAGIC ORDER BY revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT campaign_name, AVG(conversion_rate_calc) AS avg_conversion
# MAGIC FROM marketing_final
# MAGIC GROUP BY campaign_name
# MAGIC ORDER BY avg_conversion DESC;

# COMMAND ----------

from pyspark.sql.functions import sum, avg

final_table = df_final.groupBy("campaign_name") \
    .agg(
        sum("clicks").alias("total_clicks"),
        sum("impressions").alias("total_impressions"),
        sum("orders").alias("total_orders"),
        sum("mark_spent").alias("total_spend"),
        sum("csv_revenue").alias("total_revenue"),
        avg("CTR").alias("avg_ctr"),
        avg("conversion_rate_calc").alias("avg_conversion_rate"),
        avg("ROI_calc").alias("avg_roi")
    )

final_table.show()

# COMMAND ----------

final_table.orderBy("total_revenue", ascending=False).show()

# COMMAND ----------

final_table.write.mode("overwrite").saveAsTable("marketing_dashboard")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     campaign_name,
# MAGIC     SUM(clicks) AS total_clicks,
# MAGIC     SUM(impressions) AS total_impressions,
# MAGIC     SUM(orders) AS total_orders,
# MAGIC     SUM(mark_spent) AS total_spend,
# MAGIC     SUM(csv_revenue) AS total_revenue,
# MAGIC     AVG(CTR) AS avg_ctr,
# MAGIC     AVG(conversion_rate_calc) AS avg_conversion_rate,
# MAGIC     AVG(ROI_calc) AS avg_roi
# MAGIC FROM marketing_final
# MAGIC GROUP BY campaign_name
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM marketing_dashboard;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     campaign_name,
# MAGIC     SUM(clicks) AS total_clicks,
# MAGIC     SUM(impressions) AS total_impressions,
# MAGIC     SUM(orders) AS total_orders,
# MAGIC     SUM(mark_spent) AS total_spend,
# MAGIC     SUM(csv_revenue) AS total_revenue,
# MAGIC     AVG(CTR) AS avg_ctr,
# MAGIC     AVG(conversion_rate_calc) AS avg_conversion_rate,
# MAGIC     AVG(ROI_calc) AS avg_roi
# MAGIC FROM marketing_final
# MAGIC GROUP BY campaign_name
# MAGIC ORDER BY total_revenue DESC;