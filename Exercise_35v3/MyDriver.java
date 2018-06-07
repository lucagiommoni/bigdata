package it.polito.bigdata.spark.ex35;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.max;

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

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		SparkSession ss = SparkSession.builder().appName("Spark Exercise #35").getOrCreate();
		
		Dataset<Row> df = ss.read().format("csv").option("header", false).option("inferSchema", true).load(inputPath);
		
		df.as(Encoders.bean(Pollution.class)).filter("_c2=" + df.agg(max("_c2")).first().getAs("max(_c2)")).map(el->new DateVal(el.get_c1()),Encoders.bean(DateVal.class)).write().format("csv").save(outputPath);
		
		ss.stop();
	}
}
	