package com.gm.csv;

import java.io.Serializable;
import java.math.BigDecimal;

public class Record implements Serializable {

	private static final long serialVersionUID = 1L;
	private String month;
	private BigDecimal boxOffice;

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public BigDecimal getBoxOffice() {
		return boxOffice;
	}

	public void setBoxOffice(BigDecimal boxOffice) {
		this.boxOffice = boxOffice;
	}

	public Record(String month, BigDecimal boxOffice) {
		this.month = month;
		this.boxOffice = boxOffice;
	}

}
