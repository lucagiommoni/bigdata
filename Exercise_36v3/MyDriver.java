package it.polito.bigdata.spark.ex36;

import static org.apache.spark.sql.functions.avg;
import org.apache.spark.sql.*;

/**
 * 
 * MyDriver.java
 *
 * @version 3.0
 *
 * Jun 7, 2018
 * 
 * dataset
 */
public class MyDriver {

	public static void main(String[] args) {

		String inputPath = args[0];

		SparkSession ss = SparkSession.builder().appName("Spark Exercise #35").getOrCreate();
		
		Dataset<Pollution> ds = ss.read().format("csv").option("header", false).option("inferSchema", true).load(inputPath).as(Encoders.bean(Pollution.class));
		
		System.out.println((Double) ds.agg(avg("_c2")).as(Encoders.DOUBLE()).first());
		
		ss.stop();
	}
}
