package com.github.madzdns.clusterlet;

import java.net.SocketAddress;

import org.apache.mina.core.session.IoSession;

public class MinaToISession implements ISession {

	private SocketAddress remoteAddress;
	private SocketAddress localAddress;
	private boolean inStratup = true;
	
	MinaToISession(IoSession session) {
		
		this.remoteAddress = session.getRemoteAddress();
		this.localAddress = session.getLocalAddress();
		
		this.inStratup = (Boolean) session.getAttribute("startupstate");
	}
	
	@Override
	public SocketAddress getRemoteAddress() {

		return remoteAddress;
	}

	@Override
	public SocketAddress getLocalAddress() {
		
		return this.localAddress;
	}

	@Override
	public boolean isInStartup() {

		return inStratup;
	}

}
