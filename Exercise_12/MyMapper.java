package it.polito.bigdata.hadoop.ex12;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MapperEx12.java
 *
 * @version 1.0
 *
 * Apr 24, 2018
 */
public class MyMapper extends Mapper<Text, Text, Text, FloatWritable> {
	
	private Float MAX_THREASHOLD;
	
	@Override
	protected void setup(
			Mapper<Text, Text, Text, 
			FloatWritable>.Context context)
			throws IOException, InterruptedException {
		this.MAX_THREASHOLD = Float.parseFloat(context.getConfiguration().get("MAX_THRESHOLD"));
	}

	@Override
	protected void map(
			Text key, 
			Text value, 
			Mapper<Text, Text, Text, FloatWritable>.Context context)
			throws IOException, InterruptedException {
		Float tmp = Float.parseFloat(value.toString());
		if (this.MAX_THREASHOLD != null && tmp < this.MAX_THREASHOLD) {
			context.write(key, new FloatWritable(tmp));
		}
	}
}
