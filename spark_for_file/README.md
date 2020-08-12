# spark_for_file

利用spark读取txt、csv、excel文件，及简单数据分析，适用于Spark1.6.0版本

## 项目说明
  
- csv/Demo.java

  包含：读取本地或`HDFS`上的`csv`文件、过滤首行、将`JavaRDD<String>`转为`JavaRDD<Record>`、创建`DataFrame`、创建临时表和缓存表、常用统计项、数据打印、保存JSON文件


- xls/Demo.java

  包含：读取本地或`HDFS`上的`xls`文件为`JavaPairRDD<String, PortableDataStream>`、将`JavaPairRDD<String, PortableDataStream>`转为`DataInputStream`、读取`xls`文件、数据预处理、`List`数据转为`JavaRDD<Record>`对象、创建临时表和缓存表、常用统计项、数据打印、保存`JSON`文件


## spark部署方式

- YARN模式

  `mvn package`

  
  `spark-submit --master yarn --deploy-mode client --class com.gm.csv.Demo /root/spark_for_file-0.0.1-SNAPSHOT-jar-with-dependencies.jar`
  
  `spark-submit --master yarn --deploy-mode cluster --class com.gm.csv.Demo /root/spark_for_file-0.0.1-SNAPSHOT-jar-with-dependencies.jar`

  `spark-submit --master yarn --deploy-mode client --class com.gm.xls.Demo /root/spark_for_file-0.0.1-SNAPSHOT-jar-with-dependencies.jar`
  
  `spark-submit --master yarn --deploy-mode cluster --class com.gm.xls.Demo /root/spark_for_file-0.0.1-SNAPSHOT-jar-with-dependencies.jar`

- Standalone模式

  `mvn package`
  
  `spark-submit --master spark://S1:7077 --class com.gm.csv.Demo /root/spark_for_file-0.0.1-SNAPSHOT-jar-with-dependencies.jar`
  
  `spark-submit --master spark://S1:7077 --class com.gm.xls.Demo /root/spark_for_file-0.0.1-SNAPSHOT-jar-with-dependencies.jar`
  
- Local模式

  `mvn package`
  
  `spark-submit --master local[2] --class com.gm.csv.Demo /root/spark_for_file-0.0.1-SNAPSHOT-jar-with-dependencies.jar`
  
  `spark-submit --master local[2] --class com.gm.xls.Demo /root/spark_for_file-0.0.1-SNAPSHOT-jar-with-dependencies.jar`
  
  
## 重要说明

- 在通过`spark-submit模式` 提交时，需将Maven依赖打包到JAR中


- 在通过`HUE`的`Spark query`或`Workflow`时，把第三方依赖打包到`JAR`中，`Spark`相关的`JAR`通过`oozie`的`share-lib`进行共享



