package it.polito.bigdata.hadoop.ex13;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * MyCustom.java
 *
 * @version 1.0
 *
 * Apr 30, 2018
 */
public class MyCustom implements Writable {

	private String date;
	private Integer income;

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public Integer getIncome() {
		return income;
	}

	public void setIncome(Integer income) {
		this.income = income;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.date = in.readUTF();
		this.income = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.date);
		out.writeInt(this.income);
	}

}
