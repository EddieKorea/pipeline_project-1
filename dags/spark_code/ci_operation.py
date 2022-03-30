from mod.slackbot import Slack
from pyspark.sql import SparkSession

# 정윤 작성
sb = Slack('#pipeline')

try:
    spark = SparkSession.builder.master("yarn").appName("ci_operation").getOrCreate()

    ci_operation = spark.read.option("header","true").csv("/home/hjyoon/Pipeline_project/data/ci_operCode.csv")
    ci_operation.createOrReplaceTempView("ci_operation")
    ci_operation = spark.sql("select ciopercode, codeName from ci_operation")
    ci_operation.createOrReplaceTempView("ci_operation")

    ci_operation.write.option("header","true").format("csv").mode("overwrite").save("./project_data/ci_operation")
    ci_operation.coalesce(1).write.format("csv").mode("overwrite").save("./project/ci_operation")

    user="root"
    password="1234"
    url="jdbc:mysql://localhost:3306/children"
    driver="com.mysql.cj.jdbc.Driver"
    dbtable="ci_operation"

    ci_operation.write.mode("overwrite").option("truncate","true").jdbc(url, dbtable, properties={"driver": driver, "user": user, "password": password})
    sb.dbgout("ci_oper JDBC SUCCESS")
    
except Exception as ex:
    sb.dbgout("ci_oper JDBC ERROR! {str(ex)})

