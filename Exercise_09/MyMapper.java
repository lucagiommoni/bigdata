package it.polito.bigdata.hadoop.ex9;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MyMapper.java
 *
 * @version 1.0
 *
 * Apr 24, 2018
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private HashMap<String, Integer> map;
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		this.map = new HashMap<String, Integer>();		
	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		for (Entry<String , Integer> entry : this.map.entrySet()) {
			context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		String[] words = value.toString().toLowerCase().split("\\s+");
		Integer tmp;
		
		for (String word : words) {
			if ((tmp = this.map.get(word)) == null) {
				this.map.put(word, new Integer(1));
			} else {
				this.map.put(word, new Integer(tmp + 1));
			}
		}
	}
}
