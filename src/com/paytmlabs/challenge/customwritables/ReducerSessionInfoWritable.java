package com.paytmlabs.challenge.customwritables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ReducerSessionInfoWritable implements WritableComparable<ReducerSessionInfoWritable>
{

	Text sessionTime;
	IntWritable urlNum;
	IntWritable sessionNum;
	
	public ReducerSessionInfoWritable(){
		 sessionTime = new Text();
		 urlNum = new IntWritable();
		 sessionNum  = new IntWritable();
	}
	
	public ReducerSessionInfoWritable(Text sessionTime,IntWritable urlNum,IntWritable sessionNum){
		 this.sessionTime = sessionTime;
		 this.sessionNum = sessionNum;
		 this.urlNum = urlNum;
		
	}
	
	public ReducerSessionInfoWritable(String sessionTime,int urlNum,int sessionNum){
	
		 this.sessionTime = new Text(sessionTime);
		 this.urlNum = new IntWritable(urlNum);
		 this.sessionNum  = new IntWritable(sessionNum);
	}
	
	
	
	@Override
	public void readFields(DataInput in) throws IOException
	{
		sessionTime.readFields(in);
		urlNum.readFields(in);
		sessionNum.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException
	{
		sessionTime.write(out);
		urlNum.write(out);
		sessionNum.write(out);
	}
	
	@Override
	public int compareTo(ReducerSessionInfoWritable other)
	{
		int result = sessionTime.compareTo(other.getSessionTime());
			if(result == 0)
			{
				result = sessionNum.compareTo(other.getSessionNum());
				if(result == 0)
				{
					result = urlNum.compareTo(other.getUrlNum());
				}
			}
		
		return result;
		
	}


	public Text getSessionTime() {
		return sessionTime;
	}

	public void setSessionTime(Text sessionTime) {
		this.sessionTime = sessionTime;
	}

	public IntWritable getUrlNum() {
		return urlNum;
	}

	public void setUrlNum(IntWritable urlNum) {
		this.urlNum = urlNum;
	}

	public IntWritable getSessionNum() {
		return sessionNum;
	}

	public void setSessionNum(IntWritable sessionNum) {
		this.sessionNum = sessionNum;
	}
	
	public String toString()
	{
		return sessionTime+"\t"+sessionNum+"\t"+urlNum;
	}
	
}
