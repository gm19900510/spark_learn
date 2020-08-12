package com.gm.csv;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import java.math.BigDecimal;

public class Demo {
	private static String appName = "Spark_For_CSV_Demo";
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

			SQLContext sqlContext = new SQLContext(sc);

			// 从本地读取，构建rdd，movies.csv 放在项目根目录下
			JavaRDD<String> rdd = sc.textFile("movies.csv");

			// 从HDFS读取
			// JavaRDD<String> rdd = sc.textFile("hdfs://S0:8020/input/movies.csv");

			// 移除首行
			String header = rdd.first();
			rdd = rdd.filter(row -> !row.equals(header));

			// 构建JavaRDD<Record>
			JavaRDD<Record> rdd_records = rdd.map(new Function<String, Record>() {
				private static final long serialVersionUID = 1L;

				public Record call(String line) throws Exception {

					String[] fields = line.split(",");
					String month = fields[6].replace(".", "-");
					month = month.substring(0, month.length() - 3);
					System.out.println(month);
					String boxOffice = fields[3].substring(0, fields[3].length() - 1);

					BigDecimal boxOfficeNumber = new BigDecimal(boxOffice).setScale(2, BigDecimal.ROUND_HALF_UP);
					Record record = new Record(month, boxOfficeNumber);
					return record;
				}
			});

			// 创建DataFrame
			DataFrame table = sqlContext.createDataFrame(rdd_records, Record.class);

			// 注册临时表
			table.registerTempTable("record_table");
			table.printSchema();

			// 缓存临时表
			sqlContext.cacheTable("record_table");

			table.show();

			DataFrame df_result1 = sqlContext.sql(
					"select month,sum(boxOffice) sum,count(month) count from record_table group by month order by month desc limit 10");
			df_result1.show();

			// 月份的最大票房
			DataFrame df_result2 = sqlContext.sql("select max(boxOffice) max from record_table");
			df_result2.show();

			// 月份的最小票房
			DataFrame df_result3 = sqlContext.sql("select min(boxOffice) min from record_table");
			df_result3.show();

			// 月份的票房总和
			DataFrame df_result4 = sqlContext.sql("select sum(boxOffice) sum from record_table");
			df_result4.show();

			// 将结果保存为JSON
			df_result4.write().mode(SaveMode.Overwrite).json("csv.json");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (sc != null) {
				sc.close();
			}
		}
	}
}
