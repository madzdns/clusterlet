package com.github.madzdns.clusterlet;

import java.util.ArrayList;
import java.util.List;

import com.frfra.frsynch.codec.IMessage;

public class SynchProtocolOutput implements ISynchProtocolOutput {

	private List<IMessage> messages = null;
	
	SynchProtocolOutput() {
		
	}
	
	List<IMessage> getMessages() {
		
		return messages;
	}
	
	/*IMessage getMessage() {
		
		if(messages == null || messages.size() == 0) {
			
			return null;
		}
		
		return messages.get(0);
	}*/
	
	@Override
	public void write(IMessage message) {
		
		if(message == null) {
			
			messages = null;
			return;
		}
		
		this.messages = new ArrayList<>();
		this.messages.add(message);
	}

	@Override
	public void write(List<IMessage> messages) {
		
		this.messages = messages;
	}

}
