package com.paytmlabs.challenge.customwritables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey>{

	Text ip;
	LongWritable timestamp;
	
	
	
	// Constructors
	public CompositeKey()
	{
		ip = new Text();
		timestamp = new LongWritable();
	}
	
	public CompositeKey(Text ip,LongWritable timestamp)
	{
		this.ip = ip;
		this.timestamp = timestamp;
	}
	
	public CompositeKey(String ip,long timestamp)
	{
		this.ip = new Text(ip);
		this.timestamp = new LongWritable(timestamp);
	}
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		ip.readFields(in);
		timestamp.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		ip.write(out);
		timestamp.write(out);
	}
	
	@Override
	public int compareTo(CompositeKey o) {
		// TODO Auto-generated method stub
		int result = ip.compareTo(o.ip);
		if(result == 0)
		{
			result = timestamp.compareTo(o.timestamp);
		}
		return result;
	}
	
	@Override
	public String toString()
	{
		return ip.toString()+"\t"+timestamp.toString();
	}
	
	
	
	// Getters and Setters
	public Text getIp() {
		return ip;
	}
	public void setIp(Text ip) {
		this.ip = ip;
	}
	public LongWritable getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(LongWritable timestamp) {
		this.timestamp = timestamp;
	}
	
	
	
	
	
	
}
