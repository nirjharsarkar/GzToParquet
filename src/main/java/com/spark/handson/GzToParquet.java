package com.spark.handson;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.api.java.function.Function;

public class GzToParquet {

	public String getSchemaString() {
		return schemaString;
	}

	public void setSchemaString(String schemaString) {
		this.schemaString = schemaString;
	}

	public String getGzFile() {
		return gzFile;
	}

	public void setGzFile(String gzFile) {
		this.gzFile = gzFile;
	}

	public String getParquetFile() {
		return parquetFile;
	}

	public void setParquetFile(String parquetFile) {
		this.parquetFile = parquetFile;
	}

	private String schemaString;
	private String gzFile;
	private String parquetFile;

	@SuppressWarnings("serial")
	public static void main(String[] args) {

		// Should be some file on your system
		String logFile = "/tmp/gzfiles/part-m-00000.gz";
		
		// The schema is encoded in a string
		String schemaString = "entry_id sweepstakes_id sweepstakes_ep_id member_id code timestamp order_id sweepstake_name points_spent title_id list_source_id";

		//target parquet file
		String parquetFile = "/tmp/gzfiles/sweepstakes.parquet";
		
		//temp_table_name
		
		String tempTable="sweepstakes";
		
		// Create Spark Context
		SparkSession sparkCtx = SparkSession.builder().master("local").appName("GzToParquet").getOrCreate();

		// Conversion
		GzToParquet gzParquetConversion = new GzToParquet();

		gzParquetConversion.setGzFile(logFile);
		gzParquetConversion.setSchemaString(schemaString);
		gzParquetConversion.setParquetFile(parquetFile);

		gzParquetConversion.convert(sparkCtx);
		
		gzParquetConversion.showParquet(sparkCtx,tempTable);
		

	}

	public void showParquet(SparkSession sparkCtx, String tempTable) {
		
		// Read in the Parquet file.
		// Parquet files are self-describing so the schema is preserved
		// The result of loading a parquet file is also a DataFrame
		Dataset<Row> spikaSweepParquet = sparkCtx.read().parquet(getParquetFile());
		
		// Parquet files can also be used to create a temporary view and then used in SQL statements
		spikaSweepParquet.createOrReplaceTempView(tempTable);
		
		Dataset<Row> selectTable = sparkCtx.sql("SELECT * FROM "+tempTable);
		
		selectTable.show();
		
		
		
		
	}

	public void convert(SparkSession sparkCtx) {
		JavaRDD<String> spikaSweepRDD = sparkCtx.sparkContext().textFile(getGzFile(), 1).toJavaRDD();

		// String schemaString = "entry_id sweepstakes_id";

		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName : schemaString.split(" ")) {
			StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);

		// Convert records of the RDD (people) to Rows
		// For Java <8
		/*
		 * JavaRDD<Row> rowRDD = spikaSweepRDD.map(new Function<String, Row>() {
		 * public Row call(String record) throws Exception { String[] attributes
		 * = record.split("\u0001"); Object[] vals = new
		 * Object[attributes.length]; for (int i = 0; i < attributes.length;
		 * i++) { vals[i] = attributes[i].trim();
		 * 
		 * }
		 * 
		 * return RowFactory.create(vals);
		 * 
		 * } });
		 */

		// For Java >=8
		JavaRDD<Row> rowRDD = spikaSweepRDD.map(record -> {

			String[] attributes = record.split("\u0001");
			Object[] vals = new Object[attributes.length];
			for (int i = 0; i < attributes.length; i++) {
				vals[i] = attributes[i].trim();

			}

			return RowFactory.create(vals);

		});

		// Apply the schema to the RDD
		Dataset<Row> peopleDataFrame = sparkCtx.createDataFrame(rowRDD, schema);

		peopleDataFrame.write().parquet(getParquetFile());

	}

}
