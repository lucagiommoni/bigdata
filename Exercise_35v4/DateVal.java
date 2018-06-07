package it.polito.bigdata.spark.ex35;

import java.io.Serializable;
import java.sql.Timestamp;

@SuppressWarnings("serial")
public class DateVal implements Serializable {
	
	private Timestamp datetime;

	public DateVal(Timestamp datetime) {
		this.datetime = datetime;
	}

	public Timestamp getDatetime() {
		return datetime;
	}

	public void setDatetime(Timestamp datetime) {
		this.datetime = datetime;
	}
}
