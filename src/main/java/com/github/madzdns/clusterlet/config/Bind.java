package com.github.madzdns.clusterlet.config;

import java.util.List;

public class Bind {

	private List<Socket> sockets = null;

	public List<Socket> getSockets() {
		
		return sockets;
	}

	public void setSockets(List<Socket> sockets) {
		
		this.sockets = sockets;
	}
}
