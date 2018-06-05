package it.polito.bigdata.spark.ex32;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

/**
 *
 * MyDriver.java
 *
 * @version 1.0
 *
 * Jun 5, 2018
 *
 * Extract max value from csv using map and reduce functions
 */
public class MyDriver {

	public static void main(String[] args) {

		String inputPath = args[0];

		SparkConf conf = new SparkConf().setAppName("Spark ex32");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> inputText = sc.textFile(inputPath);

		JavaRDD<Float> val = inputText.map( line -> Float.parseFloat( line.split(",")[2] ) );

		Float res = val.reduce( ( e1, e2 ) -> { return e1 > e2 ? e1 : e2; } );

		System.out.println(res.toString());

		sc.close();
	}
}
