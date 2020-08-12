# hive_on_spark

利用spark操作hive，适用于Spark1.6.0版本

## spark部署方式

- YARN模式

  `mvn package`
  
  `spark-submit --master yarn --deploy-mode client --class com.gm.hive.Demo /root/hive_on_spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar`
  
  `spark-submit --master yarn --deploy-mode cluster --class com.gm.hive.Demo /root/hive_on_spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar`

- Standalone模式

  `mvn package`
  
  `spark-submit --master spark://S1:7077 --class com.gm.hive.Demo /root/hive_on_spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar`
  
- Local模式

  `mvn package`
  
  `spark-submit --master local[2] --class com.gm.hive.Demo /root/hive_on_spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar`
  
## 重要说明

- 在通过`Local模式` 或 `Standalone模式` 未提交JAR以本地测试形式访问Hive时，须在项目的`resources`目录放置`hive-site.xml`文件，未方式会出现未发现元数据异常


- 本地依赖的JAR必须与集群匹配


- 关于外部依赖JAR问题：`libs`从`spark`集群中导入，`spark`集群JAR位置：`/opt/cloudera/parcels/CDH-5.16.2-1.cdh5.16.2.p0.8/lib/oozie/share/lib/spark`


- spark-catalyst_2.10-1.6.0-cdh5.16.2.jar，从maven下载会出现异常，显示`ClassNotFoundException: org.apache.spark.sql.catalyst.rules.RuleExecutor`，因此采用从集群导入

