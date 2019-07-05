package com.github.madzdns.clusterlet.codec;

import java.util.Map;

public interface IDecoder {

	public void close();

	public void configure(Map<String, ?> config);
	
	public void deserialize(byte[] data);
}
