from mod.slackbot import Slack
from pyspark.sql import SparkSession

# 정윤 작성
sb = Slack('#pipeline')

try:
    spark = SparkSession.builder.master("yarn").appName("ci_floor_info").getOrCreate()

    ci_floor_info = spark.read.option("header","true").csv("/home/hjyoon/Pipeline_project/raw_data/ci_inst_raw.csv")
    ci_floor_info.createOrReplaceTempView("ci_floor_info")

    ci_floor_info = spark.sql("""
    select ciInstCode as ciFloorCode, 
    (case when substr(ciInst,-4)='(모래)' then '모래'
    when substr(ciInst, -7)='(고무바닥재)' then '고무바닥재' 
    when substr(ciInst, -9)='(포설도포바닥재)' then '포설도포바닥재' 
    else '기타바닥재' end) as codeName
    from ci_floor_info
    where ciInst LIKE '충격%' group by ciinstcode,ciinst order by cifloorcode""")
    ci_floor_info.createOrReplaceTempView("ci_floor_info")

    ci_floor_info.write.option("header","true").format("csv").mode("overwrite").save("./project_data/ci_floor_info")
    ci_floor_info.coalesce(1).write.format("csv").mode("overwrite").save("./project/ci_floor_info")

    user="root"
    password="1234"
    url="jdbc:mysql://localhost:3306/children"
    driver="com.mysql.cj.jdbc.Driver"
    dbtable="ci_floor_info"

    ci_floor_info.write.mode("overwrite").option("truncate","true").jdbc(url, dbtable, properties={"driver": driver, "user": user, "password": password})
    sb.dbgout("ci_floor JDBC SUCCESS")

except Exception as ex:
    sb.dbgout("ci_floor JDBC ERROR! {str(ex)})

