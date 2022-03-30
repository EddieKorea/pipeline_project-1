from mod.slackbot import Slack
from pyspark.sql import SparkSession

# 정윤 작성
sb = Slack('#pipeline')

try:
    spark = SparkSession.builder.master("yarn").appName("ci_place_info").getOrCreate()

    ci_place_info = spark.read.option("header","true").csv("/home/hjyoon/Pipeline_project/data/ci_placeCode.csv")
    ci_place_info.createOrReplaceTempView("ci_place_info")
    ci_place_info = spark.sql("select ciplacecode, codename from ci_place_info")
    ci_place_info.createOrReplaceTempView("ci_place_info")

    ci_place_info.write.option("header","true").format("csv").mode("overwrite").save("./project_data/ci_place_info")
    ci_place_info.coalesce(1).write.format("csv").mode("overwrite").save("./project/ci_place_info")

    user="root"
    password="1234"
    url="jdbc:mysql://localhost:3306/children"
    driver="com.mysql.cj.jdbc.Driver"
    dbtable="ci_place_info"

    ci_place_info.write.mode("overwrite").option("truncate","true").jdbc(url, dbtable, properties={"driver": driver, "user": user, "password": password})
    sb.dbgout("ci_place JDBC SUCCESS")

except Exception as ex:
    sb.dbgout("ci_place JDBC ERROR! {str(ex)})
