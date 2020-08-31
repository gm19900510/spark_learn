package com.gm.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Demo_Session_Mode {

	private static String appName = "Spark2_On_Hive_Demo";
	// private static String master = "local";
	private static String master = "spark://T630:7077";

	public static void main(String[] args) {
		SparkSession spark = null;
		try {
			// 初始化 JavaSparkContext，JAR提交模式
			// SparkConf conf = new SparkConf().setAppName(appName);

			// 初始化 JavaSparkContext，本地测试模式需设置Master
			SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);

			conf.set("spark.driver.host", "192.168.104.251");// 关键设置，driver端的ip地址或计算机名
			conf.set("spark.driver.bindAddress", "192.168.104.251");// driver端绑定监听block manager的ip地址或计算机名
			conf.set("spark.testing.memory", "2147480000");

			spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

			Configuration h_conf = spark.sparkContext().hadoopConfiguration();
			h_conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			h_conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

			RDD<String> rdd = spark.sparkContext().textFile("hdfs://s0:8020/input/movies.csv", 1);

			String header = rdd.first();
			System.out.println(header);

			Dataset<Row> df_result = spark.sql("select * from db_hive_test.spark_hive_java");

			df_result.show();

			// 将结果保存为JSON
			df_result.write().format("org.apache.spark.sql.execution.datasources.json.JsonFileFormat").mode(SaveMode.Overwrite).save("hdfs://s0:8020/input/df_result");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (spark != null) {
				spark.close();
			}
		}
	}
}
