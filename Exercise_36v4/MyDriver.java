package it.polito.bigdata.spark.ex36;

import org.apache.spark.sql.*;

/**
 * 
 * MyDriver.java
 *
 * @version 4.0
 *
 * Jun 7, 2018
 * 
 * sql
 */
public class MyDriver {

	public static void main(String[] args) {

		String inputPath = args[0];

		SparkSession ss = SparkSession.builder().appName("Spark Exercise #35").getOrCreate();
		
		Dataset<Pollution> ds = ss.read().format("csv").option("header", false).option("inferSchema", true).load(inputPath).as(Encoders.bean(Pollution.class));
		
		ds.createOrReplaceTempView("pollution");
		
		System.out.println(ss.sql("SELECT AVG(_c2) FROM pollution").as(Encoders.DOUBLE()).first());
		
		ss.stop();
	}
}
