package it.polito.bigdata.hadoop.ex29;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * MyReducer.java
 *
 * @version 1.0
 *
 */
public class MyReducer extends Reducer<Text, Text, Text, NullWritable> {
	
	private HashMap<String, String> users;
	private HashMap<String, LinkedList<String>> likes;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		users = new HashMap<>();
		likes = new HashMap<>();
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (String user : likes.keySet()) {
			if (likes.get(user).contains("commedia") && likes.get(user).contains("adventure")) {
				context.write(new Text(users.get(user)), NullWritable.get());
			}
		}
	}

	@Override
	protected void reduce(
			Text key,
			Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {		
		
		String[] tmp;
		LinkedList<String> tmpList;
		
		for (Text value : values) {
			tmp = value.toString().split(":");
			if (tmp[0].equalsIgnoreCase("users")) {				
				users.put(key.toString(), tmp[1]);
			} else {
				if ((tmpList = likes.get(key.toString())) == null) {
					tmpList = new LinkedList<>();
				}
				if (!tmpList.contains(tmp[1])) {					
					tmpList.add(tmp[1].toLowerCase());
				}
				likes.put(key.toString(), tmpList);
			}
		}
	}
}
