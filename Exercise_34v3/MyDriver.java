package it.polito.bigdata.spark.ex34;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.max;

/**
 * 
 * MyDriver.java
 *
 * @version 3.0
 *
 * Jun 5, 2018
 * 
 * Report the line(s) associated with the maximum value of PM10
 * Use dataframe 
 */
public class MyDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath = args[1];

		SparkSession ss = SparkSession.builder().appName("Spark Exercise #34 v3").getOrCreate();
		
		Dataset<Row> df = ss.read().format("csv").option("header", false).option("inferSchema", true).load(inputPath);
		
		df.filter("_c2=" + df.agg(max("_c2")).first().getAs("max(_c2)")).write().format("csv").save(outputPath);
		
		ss.stop();
	}
}
