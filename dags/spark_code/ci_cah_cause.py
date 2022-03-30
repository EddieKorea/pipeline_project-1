from mod.slackbot import Slack
from pyspark.sql import SparkSession

sb = Slack('#pipeline')

# 정윤 작성

try:
    spark = SparkSession.builder.master("yarn").appName("ci_cah_cause").getOrCreate()

    ci_cah_cause = spark.read.option("header","true").csv("/home/hjyoon/Pipeline_project/data/cah_cause.csv")
    ci_cah_cause.createOrReplaceTempView("ci_cah_cause")

    ci_cah_cause.write.format("csv").mode("overwrite").save("./project_data/ci_cah_cause")
    ci_cah_cause.coalesce(1).write.option("header","true").format("csv").mode("overwrite").save("./project/ci_cah_cause")

    user="root"
    password="1234"
    url="jdbc:mysql://localhost:3306/children"
    driver="com.mysql.cj.jdbc.Driver"
    dbtable="ci_cah_cause"

    ci_cah_cause.write.mode("overwrite").option("truncate","true").jdbc(url, dbtable, properties={"driver": driver, "user": user, "password": password})
    sb.dbgout("cah_cause JDBC SUCCESS")
    
except Exception as ex:
    sb.dbgout("cah_cause JDBC ERROR! {str(ex)})

