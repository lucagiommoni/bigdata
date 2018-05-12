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
public class MyMapper2 extends Mapper<Text, Text, Text, Text> {
	
	private HashMap<String, LinkedList<String>> mapOfFriends;
	private HashMap<String, LinkedList<String>> mapOfPotentialFriends;
	
	@Override
	protected void setup(Context context)throws IOException, InterruptedException {
		mapOfFriends = new HashMap<>();
		mapOfPotentialFriends = new HashMap<>();
	}

	@Override
	protected void cleanup(Context context)throws IOException, InterruptedException {
		
		HashMap<String, LinkedList<String>> tempMap;
		LinkedList<String> tmpList;
		
		for (String user : mapOfFriends.keySet()) {
			// init temp list and temp map
			tmpList = new LinkedList<>();
			tempMap = new HashMap<>();
			
			tempMap.putAll(mapOfFriends);
			tempMap.remove(user);
			
			for (String friend : mapOfFriends.get(user)) {
				tempMap.remove(friend);
			}
			
			for (String otherUser : tempMap.keySet()) {
				for (String otherUserFriend : tempMap.get(otherUser)) {
					if (mapOfFriends.get(user).contains(otherUserFriend)) {
						tmpList.add(otherUser);
						break;
					}
				}
			}
			
			mapOfPotentialFriends.put(user, tmpList);
		}
		
		String cspFriendsList;
		
		for (String user : mapOfPotentialFriends.keySet()) {
			cspFriendsList = new String();
			for (String friendsOfUser : mapOfPotentialFriends.get(user)) {
				cspFriendsList += friendsOfUser + ",";
			}
			cspFriendsList = cspFriendsList.substring(0, cspFriendsList.length()-1);
			context.write(new Text(user), new Text(cspFriendsList));
		}
	}

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		LinkedList<String> tmpList;
		
		if ((tmpList = mapOfFriends.get(key.toString())) == null) {
			tmpList = new LinkedList<>();
		}
		
		for (String user : value.toString().split(",")) {
			if (!tmpList.contains(user)) {				
				tmpList.add(user);
			}
		}
		
		mapOfFriends.put(key.toString(), tmpList);
	}

}
