package com.spark.handson;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class GzToParquetWorking {

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		String logFile = "/tmp/gzfiles/part-m-00000.gz"; // Should be
																// some file on
		// your system

		SparkConf conf = new SparkConf().setAppName("Simple App").setMaster("local");

		JavaSparkContext ctx = new JavaSparkContext(conf);

		// .config("spark.some.config.option", "some-value")
		System.out.println("Hello I'm here");

		SparkSession sqlCtx = SparkSession.builder().master("local").appName("Word Count").getOrCreate();
		System.out.println("Hello I'm still here");

		Dataset<String> peopleDF = sqlCtx.read().textFile(logFile);
		System.out.println("Hello I'm possibly here");
		
		System.out.println("---"+peopleDF.columns().length);

		peopleDF.write().parquet("/tmp/gzfiles/people.parquet");
		
		
		
		

	}

}
