package it.polito.bigdata.hadoop.ex14;

import java.util.LinkedList;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.polito.bigdata.hadoop.ex14.MyDriver.MY_COUNTERS;

/**
 * MyMapper.java
 *
 * @version 1.0
 *
 * Apr 24, 2018
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	private LinkedList<String> wordsList;

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {		
		context.getCounter(MY_COUNTERS.MAPPERS).increment(1);
		this.wordsList = new LinkedList<>();
	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {		
		for (String string : wordsList) {
			context.getCounter(MY_COUNTERS.MAPPERS_OUTPUT).increment(1);
			context.write(new Text(string), NullWritable.get());
		}
	}

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		context.getCounter(MY_COUNTERS.MAPPERS_INPUT).increment(1);
		String[] tmp = value.toString().split("\\s+");
		for (String word : tmp) {
			if (!this.wordsList.contains(word)) {
				this.wordsList.add(word);
			}
		}		
	}

}
