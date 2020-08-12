package com.gm.xls;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import com.gm.csv.Record;

import joinery.DataFrame;
import java.io.DataInputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class Demo {
	private static String appName = "Spark_For_XLS_Demo";
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

			// 将hdfs中的xls通过 binaryFiles的形式进行读取
			// JavaPairRDD<String, PortableDataStream> rdd = sc.binaryFiles("movies.xls");
			
			// 从HDFS读取
			JavaPairRDD<String, PortableDataStream> rdd = sc.binaryFiles("hdfs://S0:8020/input/movies.xls");

			// 将PortableDataStream转化为DataInputStream
			DataInputStream in = rdd.values().first().open();

			// 以xls形式进行解析
			DataFrame<Object> df = DataFrame.readXls(in);
			// 行数
			System.out.println("行数：" + df.length());
			// 空表判断
			System.out.println("空表判定：" + df.isEmpty());
			// 列数据类型
			System.out.println("列数据类型：" + df.types());
			// 列名
			System.out.println("列名：" + df.columns());

			df = df.retain("begin_date", "box_office");

			Set<Object> indexs = df.index();

			List<Record> list = new ArrayList<Record>();
			for (Object index : indexs) {
				String begin_date = df.get(index, "begin_date").toString();
				String box_office = df.get(index, "box_office").toString();

				String month = begin_date.replace(".", "-");
				month = month.substring(0, month.length() - 3);
				System.out.println(month);
				String boxOffice = box_office.substring(0, box_office.length() - 1);

				BigDecimal boxOfficeNumber = new BigDecimal(boxOffice).setScale(2, BigDecimal.ROUND_HALF_UP);
				Record record = new Record(month, boxOfficeNumber);
				list.add(record);
			}

			JavaRDD<Record> recordRDD = sc.parallelize(list);
			// 将List转为RDD对象，不然在sqlContext.cacheTable("record_table")时提示未序列化
			// “Caused by: java.io.NotSerializableException:
			// scala.collection.Iterator$$anon$11”
			
			// 创建DataFrame
			org.apache.spark.sql.DataFrame table = sqlContext.createDataFrame(recordRDD, Record.class);

			// 注册临时表
			table.registerTempTable("record_table");
			table.printSchema();
			
			// 缓存临时表
			sqlContext.cacheTable("record_table");

			table.show();

			org.apache.spark.sql.DataFrame df_result1 = sqlContext.sql(
					"select month,sum(boxOffice) sum,count(month) count from record_table group by month order by month desc limit 10");
			df_result1.show();

			// 月份的最大票房
			org.apache.spark.sql.DataFrame df_result2 = sqlContext.sql("select max(boxOffice) max from record_table");
			df_result2.show();

			// 月份的最小票房
			org.apache.spark.sql.DataFrame df_result3 = sqlContext.sql("select min(boxOffice) min from record_table");
			df_result3.show();

			// 月份的票房总和
			org.apache.spark.sql.DataFrame df_result4 = sqlContext.sql("select sum(boxOffice) sum from record_table");
			df_result4.show();

			// 将结果保存为JSON
			df_result4.write().mode(SaveMode.Overwrite).json("xls.json");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (sc != null) {
				sc.close();
			}
		}
	}
}
