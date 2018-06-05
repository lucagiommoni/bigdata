package it.polito.bigdata.spark.ex34;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.max;

/**
 * 
 * MyDriver.java
 *
 * @version 4.0
 *
 * Jun 5, 2018
 * 
 * Report the line(s) associated with the maximum value of PM10
 * Use dataset 
 */
public class MyDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath = args[1];

		SparkSession ss = SparkSession.builder().appName("Spark Exercise #34 v3").getOrCreate();
		
		Dataset<Pollution> ds = ss.read().format("csv").option("header", false).option("inferSchema", true).load(inputPath).as(Encoders.bean(Pollution.class));
		
		Double max = ds.agg(max("_c2")).as(Encoders.DOUBLE()).first();
		
		ds.filter(el -> {return el.get_c2() == max;}).write().format("csv").save(outputPath);;
		
		ss.stop();
	}
}
