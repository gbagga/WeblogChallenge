package com.paytmlabs.challenge.customwritables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ReducerCompositeValue implements WritableComparable<ReducerCompositeValue> {

	private Text timeStamp;
	private Text url;
	private Text session_id;
	private IntWritable session_no;
	
	public ReducerCompositeValue()
	{
		timeStamp = new Text();
		url = new Text();
		session_id = new Text();
		session_no = new IntWritable();
	}
	
	
	public ReducerCompositeValue(Text timeStamp,Text url, Text session_id,IntWritable session_no)
	{
		
		this.timeStamp = timeStamp;
		this.url = url;
		this.session_id = session_id;
		this.session_no = session_no;
	}
	
	public ReducerCompositeValue(String timeStamp,String url, String session_id,int session_no)
	{
		
		this.timeStamp = new Text(timeStamp);
		this.url = new Text(url);
		this.session_id = new Text(session_id);
		this.session_no = new IntWritable(session_no);
	}
	
	
	@Override
	public void readFields(DataInput in) throws IOException
	{
		
		timeStamp.readFields(in);
		url.readFields(in);
		session_id.readFields(in);
		session_no.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException
	{
		
		timeStamp.write(out);
		url.write(out);
		session_id.write(out);
		session_no.write(out);
	}
	
	@Override
	public int compareTo(ReducerCompositeValue other)
	{
		int result = timeStamp.compareTo(other.getTimeStamp());
			if(result == 0)
			{
				result = url.compareTo(other.getUrl());
				if(result == 0)
				{
					result = session_id.compareTo(other.getSession_id());
					if(result == 0)
					{
						result = session_no.compareTo(other.getSession_no());
					}
				}
			}
		
		return result;
	}

	public Text getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(Text timeStamp) {
		this.timeStamp = timeStamp;
	}

	public Text getUrl() {
		return url;
	}

	public void setUrl(Text url) {
		this.url = url;
	}

	public Text getSession_id() {
		return session_id;
	}

	public void setSession_id(Text session_id) {
		this.session_id = session_id;
	}

	public IntWritable getSession_no() {
		return session_no;
	}

	public void setSession_no(IntWritable session_no) {
		this.session_no = session_no;
	}
	
	@Override
	public String toString()
	{
		return timeStamp+"\t"+url+"\t"+session_id+"\t"+session_no;
	}
	
	
}
