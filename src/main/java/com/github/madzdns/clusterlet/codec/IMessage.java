package com.github.madzdns.clusterlet.codec;

public interface IMessage extends IEncoder, IDecoder {
	String getKey();
	long getVersion();
	int hashCode();
}
