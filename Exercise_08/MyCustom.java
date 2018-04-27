/**
 * 
 */
package it.polito.bigdata.hadoop.ex8;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * @author Luca Giommoni
 * @studentID S225049
 * @email s225049@studenti.polito.it
 * @date Apr 22, 2018 
 *
 */
public class MyCustom implements Writable {
	
	private int income;
	private String month;	

	public int getIncome() {
		return income;
	}

	public void setIncome(int income) {
		this.income = income;
	}

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.income = in.readInt();
		this.month = in.readUTF();
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.income);
		out.writeUTF(this.month);
	}

}
