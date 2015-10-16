package com.paytmlabs.challenge.customwritables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CompositeValue implements WritableComparable<CompositeValue> {

	LongWritable timestamp;
	Text url;
	Text timeStampString;
	
	
	public CompositeValue()
	{
		timestamp = new LongWritable();
		url = new Text();
		timeStampString = new Text();
		
	}
	
	public CompositeValue(LongWritable timestamp, Text url,Text timeStampString)
	{
		this.timestamp = timestamp;
		this.url = url;
		this.timeStampString = timeStampString;
	}
	
	public CompositeValue(long timestamp,String url,String timeStampString)
	{
		
		this.timestamp = new LongWritable(timestamp);
		this.url = new Text(url);
		this.timeStampString = new Text(timeStampString);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException
	{
		timestamp.readFields(in);
		url.readFields(in);
		timeStampString.readFields(in);
	}
	
	@Override 
	public void write(DataOutput out) throws IOException
	{
		timestamp.write(out);
		url.write(out);
		timeStampString.write(out);
	}
	
	@Override
	public int compareTo(CompositeValue other)
	{
		int result = timestamp.compareTo(other.timestamp);
			if(result == 0)
			{
				result = url.compareTo(other.url);
			}
		
		return result;
	}

	public LongWritable getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(LongWritable timestamp) {
		this.timestamp = timestamp;
	}

	public Text getUrl() {
		return url;
	}

	public void setUrl(Text url) {
		this.url = url;
	}

	
	public Text getTimeStampString() {
		return timeStampString;
	}

	public void setTimeStampString(Text timeStampString) {
		this.timeStampString = timeStampString;
	}

	public String toString()
	{
		return timestamp+"\t"+url+"\t"+timeStampString;
	}
	
}
