package it.polito.bigdata.spark.ex33;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

/**
 * 
 * MyDriver.java
 *
 * @version 4.0
 *
 * Jun 5, 2018
 * 
 * Extract top 3 using sql
 */
public class MyDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		
		SparkSession ss = SparkSession.builder().appName("Spark Exercise #33 v3").getOrCreate();
		
		Dataset<Pollution> dsPol = ss.read().format("csv").option("header", false).option("inferSchema", true).load(inputPath).as(Encoders.bean(Pollution.class));
		
		dsPol.createOrReplaceTempView("pollution");
		
		for(Double val : ss.sql("SELECT _c2 FROM pollution ORDER BY _c2 DESC").as(Encoders.DOUBLE()).takeAsList(3)) {
			System.out.println(val);
		}
		
		ss.stop();
	}
}
