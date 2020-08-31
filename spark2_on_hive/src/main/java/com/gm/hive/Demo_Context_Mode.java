package com.gm.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

@SuppressWarnings("deprecation")
public class Demo_Context_Mode {
	private static String appName = "Spark2_On_Hive_Demo";
	// private static String master = "local";
	private static String master = "spark://T630:7077";

	public static void main(String[] args) {

		JavaSparkContext sc = null;
		try {
			// 初始化 JavaSparkContext，JAR提交模式
			// SparkConf conf = new SparkConf().setAppName(appName);

			// 初始化 JavaSparkContext，本地测试模式需设置Master
			SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
		
			conf.set("spark.driver.host", "192.168.104.251");// 关键设置，driver端的ip地址或计算机名
			conf.set("spark.driver.bindAddress", "192.168.104.251");// driver端绑定监听block manager的ip地址或计算机名
			conf.set("spark.testing.memory", "2147480000");
			
			sc = new JavaSparkContext(conf);
			sc.setLogLevel("WARN");

			Configuration h_conf = sc.hadoopConfiguration();
			h_conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			h_conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
			
			JavaRDD<String> rdd = sc.textFile("hdfs://s0:8020/input/movies.csv");

			String header = rdd.first();
			System.out.println(header);

			HiveContext hiveContext = new HiveContext(sc);

			Dataset<Row> df_result = hiveContext.sql("show databases");

			df_result.show();

			// 将结果保存为JSON
			df_result.write().format("org.apache.spark.sql.execution.datasources.json.JsonFileFormat").mode(SaveMode.Overwrite).save("hdfs://s0:8020/input/df_result");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (sc != null) {
				sc.close();
			}
		}
	}
}
