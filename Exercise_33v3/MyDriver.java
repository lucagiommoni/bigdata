package it.polito.bigdata.spark.ex33;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

/**
 *
 * MyDriver.java
 *
 * @version 3.0
 *
 * Jun 5, 2018
 *
 * Extract top 3 using dataset
 */
public class MyDriver {

	public static void main(String[] args) {

		String inputPath = args[0];

		SparkSession ss = SparkSession.builder().appName("Spark Exercise #33 v3").getOrCreate();

		Dataset<Pollution> valPoll = ss.read().format("csv").option("header", false).option("inferSchema", true).load(inputPath).as(Encoders.bean(Pollution.class));

		Dataset<PM10> pm10 = valPoll.map(val -> new PM10(val.get_c2()), Encoders.bean(PM10.class));

		for(Double val : pm10.sort(new Column("pm10val").desc()).as(Encoders.DOUBLE()).takeAsList(3)) {
			System.out.println(val);
		}

		ss.stop();
	}
}
