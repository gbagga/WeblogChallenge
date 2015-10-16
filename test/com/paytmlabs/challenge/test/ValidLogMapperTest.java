package com.paytmlabs.challenge.test;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import com.paytmlabs.challenge.WeblogMapper;
import com.paytmlabs.challenge.customwritables.CompositeKey;
import com.paytmlabs.challenge.customwritables.CompositeValue;

public class ValidLogMapperTest {

	@Test
	public void processValidRecord() throws IOException,InterruptedException
	{
	
		Text value = new Text("2015-07-22T09:00:28.268027Z marketpalce-shop 14.97.21.122:61543 10.0.6.108:80 0.000023 0.010536 0.000032 200 200 0 223 \"GET https://paytm.com:443/shop/cart?channel=web&version=2 HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2");
		new MapDriver<LongWritable,Text,CompositeKey,CompositeValue>()
		.withMapper(new WeblogMapper())
		.withInput(new LongWritable(0), value)
		.runTest();
		
		
	}
}
