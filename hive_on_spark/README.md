# spark_on_hive

利用spark操作hive，适用于Spark1.6.0版本

## spark部署方式

- YARN模式

  `mvn package`
  
  `spark-submit --master yarn --deploy-mode client --class com.gm.hive.Demo /root/spark_on_hive-0.0.1-SNAPSHOT-jar-with-dependencies.jar`
  
  `spark-submit --master yarn --deploy-mode cluster --class com.gm.hive.Demo /root/spark_on_hive-0.0.1-SNAPSHOT-jar-with-dependencies.jar`

- Standalone模式

  `mvn package`
  
  `spark-submit --master spark://S1:7077 --class com.gm.hive.Demo /root/spark_on_hive-0.0.1-SNAPSHOT-jar-with-dependencies.jar`
  
- Local模式

  `mvn package`
  
  `spark-submit --master local[2] --class com.gm.hive.Demo /root/spark_on_hive-0.0.1-SNAPSHOT-jar-with-dependencies.jar`
  
## 重要说明

- 在通过`Local模式` 或 `Standalone模式` 未提交JAR以本地测试形式访问Hive时，须在项目的`resources`目录放置`hive-site.xml`文件，未放置会出现未发现元数据异常


- 依赖的JAR必须与集群匹配
