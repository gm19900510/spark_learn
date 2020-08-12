package com.gm.rdd;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Demo {
	private static String appName = "Spark_For_RDD_Demo";
	private static String master = "local";
	// private static String master = "spark://S1:7077";

	public static void main(String[] args) {

		JavaSparkContext sc = null;
		try {
			// 初始化 JavaSparkContext，JAR提交模式
			// SparkConf conf = new SparkConf().setAppName(appName);

			// 初始化 JavaSparkContext，本地测试模式需设置Master
			SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);

			sc = new JavaSparkContext(conf);
			sc.setLogLevel("WARN");

			List<Integer> list = Arrays.asList(5, 4, 3, 2, 1);
			JavaRDD<Integer> rdd = sc.parallelize(list);

			rdd.collect().forEach(System.out::println);

			long count = rdd.count();
			System.out.println(count);

			rdd.saveAsTextFile("hdfs://s0:8020/input/list");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (sc != null) {
				sc.close();
			}
		}
	}
}
