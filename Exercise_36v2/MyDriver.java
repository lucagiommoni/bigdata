package it.polito.bigdata.spark.ex36;

import static org.apache.spark.sql.functions.avg;
import org.apache.spark.sql.*;

/**
 * 
 * MyDriver.java
 *
 * @version 2.0
 *
 * Jun 7, 2018
 * 
 * dataframe
 */
public class MyDriver {

	public static void main(String[] args) {

		String inputPath = args[0];

		SparkSession ss = SparkSession.builder().appName("Spark Exercise #35").getOrCreate();
		
		Dataset<Row> df = ss.read().format("csv").option("header", false).option("inferSchema", true).load(inputPath);
		
		System.out.println((Double) df.agg(avg("_c2")).first().getAs("avg(_c2)"));
		
		ss.stop();
	}
}
