from mod.slackbot import Slack
from pyspark.sql import SparkSession

# 정윤 작성
sb = Slack('#pipeline')

try:
    spark = SparkSession.builder.master("yarn").appName("ci_accident").getOrCreate()

    ci_accident = spark.read.option("header","true").csv("/home/hjyoon/Pipeline_project/data/ci_accident.csv")
    ci_accident.createOrReplaceTempView("ci_accident")
    ci_accident = spark.sql("select cast(cahseq as integer) as cahseq, cahdate, cahcontent, cahaction, cahCode, cicode from ci_accident order by cahseq")
    ci_accident.createOrReplaceTempView("ci_accident")

    ci_accident.write.format("csv").mode("overwrite").save("./project_data/ci_accident")
    ci_accident.coalesce(1).write.format("csv").mode("overwrite").option("header","true").save("./project/ci_accident")

    user="root"
    password="1234"
    url="jdbc:mysql://localhost:3306/children"
    driver="com.mysql.cj.jdbc.Driver"
    dbtable="ci_accident"

    ci_accident.write.mode("overwrite").option("truncate","true").jdbc(url, dbtable, properties={"driver": driver, "user": user, "password": password})
    sb.dbgout("ci_accident JDBC SUCCESS")

except Exception as ex:
    sb.dbgout(f"ci_accident JDBC ERROR! {str(ex)})

