package it.polito.bigdata.spark.ex33;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 
 * MyDriver.java
 *
 * @version 2.0
 *
 * Jun 5, 2018
 * 
 * Extract top 3 using dataframe
 */
public class MyDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		
		SparkSession ss = SparkSession.builder().appName("Spark Exercise #33 v2").getOrCreate();
		
		for(Row val : ss.read().format("csv").option("header", false).option("inferSchema", true).load(inputPath).select("_c2").sort(new Column("_c2").desc()).takeAsList(3)) {
			System.out.println((Double)val.getAs("_c2"));
		}
		
		ss.stop();
	}
}
