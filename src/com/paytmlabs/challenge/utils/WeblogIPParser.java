package com.paytmlabs.challenge.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class WeblogIPParser {

	private long timeStamp;
	private String timeStampString;
	private String clientIp;
	private boolean validWebLog;
	private String url;
	private String userAgent;
	
	private String  elb;
	private String  clientPort;
	private String  backendIpPort ;
	private String  requestProcessingTime ;
	private String  backendProcessingTime ;
	private String  responseProcessingTime ;
	private String  elb_status_code;
	private String  backend_status_code;
	private String  received_bytes ;
	private String  sent_bytes;
	private String  requestMethod ;
	private String  requestProtocol ;
	private String  sslCipher ;
	private String  sslProtocol ;
	
	
	//InetAddressValidator validator = InetAddressValidator.getInstance();
	
	
	public boolean isValidWebLog() {
		return validWebLog;
	}

	public void setValidWebLog(boolean validWebLog) {
		this.validWebLog = validWebLog;
	}

	public long getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}

	public String getClientIp() {
		return clientIp;
	}

	public void setClientIp(String clientIp) {
		this.clientIp = clientIp;
	}

	
	
	public String getTimeStampString() {
		return timeStampString;
	}

	public void setTimeStampString(String timeStampString) {
		this.timeStampString = timeStampString;
	}

	public String getElb() {
		return elb;
	}

	public void setElb(String elb) {
		this.elb = elb;
	}

	public String getClientPort() {
		return clientPort;
	}

	public void setClientPort(String clientPort) {
		this.clientPort = clientPort;
	}

	public String getBackendIpPort() {
		return backendIpPort;
	}

	public void setBackendIpPort(String backendIpPort) {
		this.backendIpPort = backendIpPort;
	}

	public String getRequestProcessingTime() {
		return requestProcessingTime;
	}

	public void setRequestProcessingTime(String requestProcessingTime) {
		this.requestProcessingTime = requestProcessingTime;
	}

	public String getBackendProcessingTime() {
		return backendProcessingTime;
	}

	public void setBackendProcessingTime(String backendProcessingTime) {
		this.backendProcessingTime = backendProcessingTime;
	}

	public String getResponseProcessingTime() {
		return responseProcessingTime;
	}

	public void setResponseProcessingTime(String responseProcessingTime) {
		this.responseProcessingTime = responseProcessingTime;
	}

	public String getElb_status_code() {
		return elb_status_code;
	}

	public void setElb_status_code(String elb_status_code) {
		this.elb_status_code = elb_status_code;
	}

	public String getBackend_status_code() {
		return backend_status_code;
	}

	public void setBackend_status_code(String backend_status_code) {
		this.backend_status_code = backend_status_code;
	}

	public String getReceived_bytes() {
		return received_bytes;
	}

	public void setReceived_bytes(String received_bytes) {
		this.received_bytes = received_bytes;
	}

	public String getSent_bytes() {
		return sent_bytes;
	}

	public void setSent_bytes(String sent_bytes) {
		this.sent_bytes = sent_bytes;
	}

	public String getRequestMethod() {
		return requestMethod;
	}

	public void setRequestMethod(String requestMethod) {
		this.requestMethod = requestMethod;
	}

	public String getRequestProtocol() {
		return requestProtocol;
	}

	public void setRequestProtocol(String requestProtocol) {
		this.requestProtocol = requestProtocol;
	}

	public String getSslCipher() {
		return sslCipher;
	}

	public void setSslCipher(String sslCipher) {
		this.sslCipher = sslCipher;
	}

	public String getSslProtocol() {
		return sslProtocol;
	}

	public void setSslProtocol(String sslProtocol) {
		this.sslProtocol = sslProtocol;
	}

	public void parseWeblog(String weblog)
	{
	
		Pattern r = Pattern.compile("([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) (-|[^ ]*:[0-9]*) (-1|[.0-9]*) (-1|[.0-9]*) (-1|[.0-9]*) (-|[0-9]*) (-|[0-9]*) ([-0-9]*) ([-0-9]*) \\\"([^ ]*) ([^ ]*) (- |[^ ]*)\\\" \\\"(.*)\\\" (.*) (.*)$");
		Matcher m = r.matcher(weblog);
		System.out.println(m.groupCount());
		
		if (m.find()) {
            int groupSize = m.groupCount();
            if (groupSize == 18) {
            	
            	validWebLog = true;
            	
            	 timeStampString = m.group(1);
            	 timeStamp = (new DateTime(timeStampString, DateTimeZone.UTC)).getMillis() / 1000;
            	 elb = m.group(2);
            	 clientIp = m.group(3);
            	 clientPort = m.group(4);
            	 backendIpPort = m.group(5);
            	 requestProcessingTime = m.group(6);
            	 backendProcessingTime = m.group(7);
            	 responseProcessingTime = m.group(8);
            	 elb_status_code = m.group(9);
            	 backend_status_code = m.group(10);
            	 received_bytes = m.group(11);
            	 sent_bytes = m.group(12);
            	 requestMethod = m.group(13);
            	 url = m.group(14);
            	 requestProtocol = m.group(15);
            	 userAgent = m.group(16);
            	 sslCipher = m.group(17);
            	 sslProtocol = m.group(18);
            	   
                }else
                {
                	validWebLog = false;
                }
           
		}else
		{
			validWebLog = false; 
		}
	}
	
	public void parse(Text weblog)
	{
		parseWeblog(weblog.toString());
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUserAgent() {
		return userAgent;
	}

	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
	}
	
	
	public static void main(String[] args)
	{
		
		BufferedReader br = null;
		try{
			
		
		WeblogIPParser wip = new WeblogIPParser();
		 br = new BufferedReader(new FileReader("C:\\Users\\Gaurav\\Desktop\\CanadaJobs\\paytmlabs challenge\\WeblogChallenge-master\\WeblogChallenge-master\\data\\2015_07_22_mktplace_shop_web_log_sample.log\\newfile.txt"));
		String line = null;
		while((line=br.readLine())!= null)
		{
			System.out.println(line);
			wip.parseWeblog(line);
		}
		
		}catch(Exception e)
		{
			e.printStackTrace();
		}finally
		{
			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	
}
