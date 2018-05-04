package it.polito.bigdata.hadoop.ex17;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * MyReducer.java
 *
 * @version 1.0
 *
 */
public class MyReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

	@Override
	protected void reduce(
			Text key,
			Iterable<FloatWritable> values,
			Context context)
			throws IOException, InterruptedException {
		
		Float max = Float.MIN_VALUE;
		
		for (FloatWritable value : values) {
			if (max < value.get()) {
				max = value.get();
			}
		}
		
		context.write(key, new FloatWritable(max));
	}
}
