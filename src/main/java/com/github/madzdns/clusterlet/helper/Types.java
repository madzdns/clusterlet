package com.github.madzdns.clusterlet.helper;

public class Types {

	public final static int Bytes;
	public final static int ShortBytes; 
	public final static int CharBytes;
	public final static int LongBytes;
	
	static {
		
		Bytes = (Byte.SIZE/8);
		ShortBytes=(Short.SIZE/8);
		CharBytes=(Character.SIZE/8);
		LongBytes=(Long.SIZE/8);
	}
}
