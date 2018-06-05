package it.polito.bigdata.spark.ex32;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.max;

/**
 *
 * MyDriver.java
 *
 * @version 3.0
 *
 * Jun 5, 2018
 *
 * Extract max value from csv using dataset
 */
public class MyDriver {

	public static void main(String[] args) {

		String inputPath = args[0];

		SparkSession ss = SparkSession.builder().appName("Spark ex32").getOrCreate();

		Dataset<Pollution> dsPoll = ss.read().format("csv").option("header", false).option("inferSchema", true).load(inputPath).as(Encoders.bean(Pollution.class));

		Dataset<Double> maxPoll = dsPoll.agg(max("_c2")).as(Encoders.DOUBLE());

		Double max = maxPoll.first();

		System.out.println(max);

		ss.stop();
	}
}
