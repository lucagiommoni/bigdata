package it.polito.bigdata.hadoop.ex23;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * MyReducer.java
 *
 * @version 1.0
 *
 */
public class MyReducer extends Reducer<NullWritable, Text, Text, NullWritable> {


	@Override
	protected void reduce(
			NullWritable key,
			Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {

		LinkedList<String> potentialFriendsList = new LinkedList<>();
		
		for (Text value : values) {
			String[] partialPotentialFriendsList = value.toString().split(",");
			for (String potentialFriend : partialPotentialFriendsList) {
				if (!potentialFriendsList.contains(potentialFriend)) {
					potentialFriendsList.add(potentialFriend);
				}
			}
		}
		
		String commaSeparatedList = "";
		
		for (String potentialFriend : potentialFriendsList) {
			commaSeparatedList += potentialFriend + ",";
		}
		
		commaSeparatedList = commaSeparatedList.substring(0, commaSeparatedList.length()-1);
		
		context.write(new Text(commaSeparatedList), NullWritable.get());
	}
}
