package com.github.madzdns.clusterlet.config;

public class Socket {

	public static final String ANY = "*";

	private String ip = null;
	
	private int port = 0;
	
	private String value=null;
	
	public String getIp() {
		if(ip!=null)
			return ip;
		if(value==null||value.split(":").length!=2)
			return null;
		return ip = value.split(":")[0];
	}
	
	public int getPort() {
		
		if(port!=0)
			return port;
		
		if(value==null || 
				value.split(":").length !=2 )
			
			return 0;
		
		return port = Integer.parseInt(value.split(":")[1]);
	}
	
	public String getValue() {
		
		return value;
	}
	
	public void setValue(String value) {
		
		this.value = value;
	}
	
	@Override
	public String toString() {

		return value;
	}
}
