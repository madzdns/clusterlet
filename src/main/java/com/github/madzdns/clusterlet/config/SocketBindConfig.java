package com.github.madzdns.clusterlet.config;

import java.util.List;

public class SocketBindConfig {
	private List<SocketConfig> socketConfigs = null;
	public List<SocketConfig> getSocketConfigs() {
		return socketConfigs;
	}

	public void setSocketConfigs(List<SocketConfig> socketConfigs) {
		this.socketConfigs = socketConfigs;
	}
}
