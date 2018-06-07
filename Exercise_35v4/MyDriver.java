package it.polito.bigdata.spark.ex35;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.max;

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

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		SparkSession ss = SparkSession.builder().appName("Spark Exercise #35").getOrCreate();
		
		Dataset<Pollution> ds = ss.read().format("csv").option("header", false).option("inferSchema", true).load(inputPath).as(Encoders.bean(Pollution.class));
		
		ds.createOrReplaceTempView("pollution");
		
		Double max = ss.sql("SELECT MAX(_c2) FROM pollution").as(Encoders.DOUBLE()).first();
		
		ss.sql("SELECT _c1 as datetime FROM pollution WHERE _c2="+max).as(Encoders.bean(DateVal.class)).write().format("csv").save(outputPath);
		
		ss.stop();
	}
}
	