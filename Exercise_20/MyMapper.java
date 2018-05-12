package it.polito.bigdata.hadoop.ex20;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * MapperEx12.java
 *
 * @version 1.0
 *
 * Apr 24, 2018
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	private MultipleOutputs<Text, NullWritable> mOutputs;	
	private Float MAX_THREASHOLD;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		mOutputs = new MultipleOutputs<>(context);
		this.MAX_THREASHOLD = Float.parseFloat(context.getConfiguration().get("MAX_THRESHOLD"));
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		mOutputs.close();		
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split(",");
		Float tmp = Float.parseFloat(fields[3]);
		if (tmp < this.MAX_THREASHOLD) {
			mOutputs.write("normaltemp", value, NullWritable.get());
		} else {
			mOutputs.write("hightemp", value, NullWritable.get());
		}
	}
}
