package it.polito.bigdata.hadoop.ex;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import it.polito.bigdata.hadoop.ex14.MyDriver.MY_COUNTERS;

/**
 * MyReducer.java
 *
 * @version 1.0
 *
 * Apr 24, 2018
 */
public class MyReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

	@Override
	protected void setup(Reducer<Text, NullWritable, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		context.getCounter(MY_COUNTERS.REDUCERS).increment(1);
	}

	@Override
	protected void cleanup(Reducer<Text, NullWritable, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
	}

	@Override
	@SuppressWarnings("unused")
	protected void reduce(Text key, Iterable<NullWritable> values,
			Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {

		for (NullWritable nullWritable : values) {
			context.getCounter(MY_COUNTERS.REDUCERS_INPUT).increment(1);
		}
		context.getCounter(MY_COUNTERS.REDUCERS_OUTPUT).increment(1);
		context.write(key, NullWritable.get());
	}
}
