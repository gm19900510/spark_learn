package com.gm.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

public class Demo {
	private static String appName = "Spark_On_Hive_Demo";
	// private static String master = "local";
	private static String master = "spark://S1:7077";

	public static void main(String[] args) {

		JavaSparkContext sc = null;
		try {
			// 初始化 JavaSparkContext，JAR提交模式
			// SparkConf conf = new SparkConf().setAppName(appName);

			// 初始化 JavaSparkContext，本地测试模式需设置Master
			SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);

			sc = new JavaSparkContext(conf);
			sc.setLogLevel("WARN");

			Configuration h_conf = sc.hadoopConfiguration();
			h_conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			h_conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
			
			JavaRDD<String> rdd = sc.textFile("hdfs://s0:8020/input/movies.csv");

			String header = rdd.first();
			System.out.println(header);

			HiveContext hiveContext = new HiveContext(sc);

			DataFrame df_result = hiveContext.sql("select * from db_hive_test.spark_hive_java");

			df_result.show();

			// 将结果保存为JSON
			df_result.write().mode(SaveMode.Overwrite).json("hdfs://s0:8020/input/df_result");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (sc != null) {
				sc.close();
			}
		}
	}
}
