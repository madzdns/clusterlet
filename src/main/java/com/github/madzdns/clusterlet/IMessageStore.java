package com.github.madzdns.clusterlet;

import java.util.Set;

public interface IMessageStore {

	public void updateAwareNodes(String key, long version, Set<Short> awareNodes);
	
	public Set<Short> getAwareNodes(String key, long version);
	
	public void shutdown();
}
