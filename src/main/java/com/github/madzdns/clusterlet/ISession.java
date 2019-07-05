package com.github.madzdns.clusterlet;

import java.net.SocketAddress;

public interface ISession {

	public SocketAddress getRemoteAddress();
	public SocketAddress getLocalAddress();
	public boolean isInStartup();
}
