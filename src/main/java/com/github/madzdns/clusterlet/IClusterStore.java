package com.github.madzdns.clusterlet;

public interface IClusterStore {

	public void update(Member node);
	
	public Member get(Short id);
	
	public void shutdown();
	
	public void iterator(IClusterStoreIteratorCallback callbak);
}
