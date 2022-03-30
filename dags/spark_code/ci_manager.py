from mod.slackbot import Slack
from pyspark.sql import SparkSession

# 정윤 작성
sb = Slack('#pipeline')

try:
    spark = SparkSession.builder.master("yarn").appName("ci_manager").getOrCreate()

    ci_manager = spark.read.option("header","true").csv("/home/hjyoon/Pipeline_project/raw_data/ci_manager_raw.csv")
    ci_manager.createOrReplaceTempView("ci_manager")

    ci_manager = spark.sql("select cast(csSeq as integer) as scseq,ciseq,ciindgroup from ci_manager where ciindgroup is not null order by scseq")
    ci_manager.createOrReplaceTempView("ci_manager")

    ci_manager.write.option("header","true").format("csv").mode("overwrite").save("./project_data/ci_manager")
    ci_manager.coalesce(1).write.format("csv").mode("overwrite").save("./project/ci_manager")

    user="root"
    password="1234"
    url="jdbc:mysql://localhost:3306/children"
    driver="com.mysql.cj.jdbc.Driver"
    dbtable="ci_manager"

    ci_manager.write.mode("overwrite").option("truncate","true").jdbc(url, dbtable, properties={"driver": driver, "user": user, "password": password})
    sb.dbgout("ci_manager JDBC SUCCESS")

except Exception as ex:
    sb.dbgout("ci_manager JDBC ERROR! {str(ex)})
