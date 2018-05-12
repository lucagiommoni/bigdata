package it.polito.bigdata.hadoop.ex25;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MyMapper.java
 *
 * @version 1.0
 *
 */
public class MyMapper1 extends Mapper<LongWritable, Text, Text, Text> {
	
	private HashMap<String, LinkedList<String>> map;
	
	@Override
	protected void setup(Context context)throws IOException, InterruptedException {
		map = new HashMap<>();
	}

	@Override
	protected void cleanup(Context context)throws IOException, InterruptedException {
		String tmp;
		for (String user : map.keySet()) {
			tmp = new String();
			for (String friend : map.get(user)) {
				tmp += friend + ",";
			}
			tmp = tmp.substring(0,  tmp.length()-1);
			context.write(new Text(user), new Text(tmp));
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] user = value.toString().toLowerCase().split(",");
		LinkedList<String> user_friends_list;
		
		if ((user_friends_list = map.get(user[0])) == null) {
			user_friends_list = new LinkedList<>();
		}
		user_friends_list.add(user[1]);
		map.put(user[0], user_friends_list);
		
		if ((user_friends_list = map.get(user[1])) == null) {
			user_friends_list = new LinkedList<>();
		}
		user_friends_list.add(user[0]);
		map.put(user[1], user_friends_list);
	}

}
