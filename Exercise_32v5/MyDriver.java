package it.polito.bigdata.spark.ex32;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * MyDriver.java
 *
 * @version 5.0
 *
 * Jun 5, 2018
 *
 * Extract max value from csv using map and top functions
 */
public class MyDriver {

	public static void main(String[] args) {

		String inputPath = args[0];

		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Exercise #32 v5"));

		JavaRDD<String> jRDD = sc.textFile(inputPath);

		JavaRDD<Double> maxjRDD = jRDD.map(el -> (Double)Double.parseDouble(el.split(",")[2]));

		System.out.println(maxjRDD.top(1).get(0));

		sc.close();
	}
}
