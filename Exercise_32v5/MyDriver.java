package it.polito.bigdata.spark.ex32;

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
 * Extract max value from csv using sql
 */
public class MyDriver {

	public static void main(String[] args) {

		String inputPath = args[0];

		SparkSession ss = SparkSession.builder().appName("Spark ex32").getOrCreate();

		Dataset<Pollution> dsPoll = ss.read().format("csv").option("header", false).option("inferSchema", true).load(inputPath).as(Encoders.bean(Pollution.class));

		dsPoll.createOrReplaceTempView("pollution");

		Dataset<Double> maxPoll = ss.sql("SELECT MAX(_c2) FROM pollution").as(Encoders.DOUBLE());

		Double max = maxPoll.first();

		System.out.println(max);

		ss.stop();
	}
}
