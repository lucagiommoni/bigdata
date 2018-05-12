package it.polito.bigdata.hadoop.ex23;

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
public class MyMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	private HashMap<String, LinkedList<String>> map;
	private String user;
	
	@Override
	protected void setup(Context context)throws IOException, InterruptedException {
		map = new HashMap<>();
		user = context.getConfiguration().get("username").toLowerCase();
	}

	@Override
	protected void cleanup(Context context)throws IOException, InterruptedException {
		LinkedList<String> userFriendsList = map.remove(user);
		LinkedList<String> otherUser_friends_list;
		String potentialFriends = "";
		
		for (String friend_of_user : userFriendsList) {
			map.remove(friend_of_user);
		}		
		
		for (String otherUser : map.keySet()) {
			
			otherUser_friends_list = map.get(otherUser);
			
			for (String friend_of_otherUser : otherUser_friends_list) {
				if (userFriendsList.contains(friend_of_otherUser)) {
					potentialFriends += otherUser + ",";
					break;
				}
			}
		}
		
		potentialFriends = potentialFriends.substring(0, potentialFriends.length()-1);
		
		context.write(NullWritable.get(), new Text(potentialFriends));
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
