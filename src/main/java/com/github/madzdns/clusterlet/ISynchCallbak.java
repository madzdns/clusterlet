package com.github.madzdns.clusterlet;

import java.util.Set;

import com.frfra.frsynch.codec.IMessage;

public interface ISynchCallbak {

	public boolean callBack(ISession session, IMessage message,
                            Set<Short> withNodes, ISynchProtocolOutput out);
	
	public void result(SynchFeature synchFeature);
	
	//public boolean createUpdate(String key, long version, ISynchProtocolOutput out);
}
