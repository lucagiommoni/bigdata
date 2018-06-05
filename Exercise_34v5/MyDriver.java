package it.polito.bigdata.spark.ex34;

import org.apache.spark.sql.*;

/**
 * 
 * MyDriver.java
 *
 * @version 5.0
 *
 * Jun 5, 2018
 * 
 * Report the line(s) associated with the maximum value of PM10
 * Use sql 
 */
public class MyDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath = args[1];

		SparkSession ss = SparkSession.builder().appName("Spark Exercise #34 v3").getOrCreate();
		
		Dataset<Pollution> ds = ss.read().format("csv").option("header", false).option("inferSchema", true).load(inputPath).as(Encoders.bean(Pollution.class));
		
		ds.createOrReplaceTempView("pollution");
		
		Double max = (Double)ss.sql("SELECT MAX(_c2) FROM pollution").as(Encoders.DOUBLE()).first();
		
		ss.sql("SELECT * FROM pollution WHERE _c2=" + max.toString()).as(Encoders.bean(Pollution.class)).write().format("csv").save(outputPath);
		
		ss.stop();
	}
}
