package com.github.madzdns.clusterlet;

import java.util.List;

import com.frfra.frsynch.codec.IMessage;

public interface ISynchProtocolOutput {

	public void write(IMessage message);
	
	public void write(List<IMessage> message);
}
