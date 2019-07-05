package com.github.madzdns.clusterlet;

public interface IClusterStore {

	public void update(ClusterNode node);
	
	public ClusterNode get(Short id);
	
	public void shutdown();
	
	public void iterator(IClusterStoreIteratorCallback callbak);
}
