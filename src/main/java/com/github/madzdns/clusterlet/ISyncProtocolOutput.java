package com.github.madzdns.clusterlet;

import java.util.List;

import com.github.madzdns.clusterlet.codec.IMessage;

public interface ISyncProtocolOutput {

	public void write(IMessage message);
	
	public void write(List<IMessage> message);
}
