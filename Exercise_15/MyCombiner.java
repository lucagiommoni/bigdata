package it.polito.bigdata.hadoop.ex15;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import it.polito.bigdata.hadoop.ex15.MyDriver.MY_COUNTERS;

/**
 * MyCombiner.java
 *
 * @version 1.0
 *
 * Apr 24, 2018
 */
public class MyCombiner extends Reducer<Text, NullWritable, Text, NullWritable> {

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		context.getCounter(MY_COUNTERS.COMBINERS).increment(1);
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
	}

	@Override
	@SuppressWarnings("unused")
	protected void reduce(
			Text key,
			Iterable<NullWritable> values,
			Context context) 
			throws IOException, InterruptedException {

		for (NullWritable nullWritable : values) {
			context.getCounter(MY_COUNTERS.COMBINERS_INPUT).increment(1);
		}
		context.getCounter(MY_COUNTERS.COMBINERS_OUTPUT).increment(1);
		context.write(key, NullWritable.get());
	}
}
