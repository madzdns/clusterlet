package com.github.madzdns.clusterlet;

public interface INode {

	public short getId();
	public byte getVersion();
	public void setVersion(byte version);
}
