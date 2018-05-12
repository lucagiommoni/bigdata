package it.polito.bigdata.hadoop.ex24;

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
public class MyReducer extends Reducer<Text, Text, Text, Text> {


	@Override
	protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
		LinkedList<String> friends = new LinkedList<>();
		String[] tmpFriends;
		
		for (Text value : values) {
			tmpFriends = value.toString().split(",");
			for (String tmpFriend : tmpFriends) {			
				if (!friends.contains(tmpFriend)) {
					friends.add(tmpFriend);
				}
			}			
		}
		
		String res = "";
		
		for (String friend : friends) {
			res += friend + ",";
		}
		
		res = res.substring(0, res.length()-1);
		context.write(key, new Text(res));
	}
}
