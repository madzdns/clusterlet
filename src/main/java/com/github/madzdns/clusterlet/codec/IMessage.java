package com.github.madzdns.clusterlet.codec;

public interface IMessage extends IEncoder, IDecoder {

	public String getKey();
	
	public long getVersion();
}
