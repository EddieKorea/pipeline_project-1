from mod.slackbot import Slack
from pyspark.sql import SparkSession

# 정윤 작성
sb = Slack('#pipeline')

try:
    spark = SparkSession.builder.master("yarn").appName("ci_inst").getOrCreate()

    ci_inst = spark.read.option("header","true").csv("/home/hjyoon/Pipeline_project/raw_data/ci_inst_raw.csv")
    ci_inst.createOrReplaceTempView("ci_inst")

    ci_inst.write.option("header","true").format("csv").mode("overwrite").save("./project_data/ci_inst")
    ci_inst.coalesce(1).write.format("csv").mode("overwrite").save("./project/ci_inst")

    user="root"
    password="1234"
    url="jdbc:mysql://localhost:3306/children"
    driver="com.mysql.cj.jdbc.Driver"
    dbtable="ci_inst"

    ci_inst.write.mode("overwrite").option("truncate","true").jdbc(url, dbtable, properties={"driver": driver, "user": user, "password": password})
    sb.dbgout("ci_inst JDBC SUCCESS")
    
except Exception as ex:
    sb.dbgout("ci_inst JDBC ERROR! {str(ex)})
