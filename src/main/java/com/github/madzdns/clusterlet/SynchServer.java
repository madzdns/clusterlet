package com.github.madzdns.clusterlet;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.madzdns.clusterlet.ClusterNode.ClusterAddress;
import com.github.madzdns.clusterlet.config.Bind;
import com.github.madzdns.clusterlet.config.Socket;

public class SynchServer {

	private Logger log = LoggerFactory.getLogger(SynchServer.class);
	
	private Bind synchBindings;
	private Bind backendBindings;
	private SynchHandler handler;
	private SynchContext synchContext;
	
	public SynchServer(SynchHandler handler, Bind synchBindings,
			Bind backendBindings) {
		
		this.synchBindings = synchBindings;
		this.handler = handler;
		this.backendBindings = backendBindings;
		this.synchContext = handler.synchContext;
	}
	
	public void start() throws IOException {
		
		if(synchBindings == null || handler == null 
				|| synchContext == null) {
			
			return;
		}
		
		if(handler.starter) {
			
			throw new IllegalStateException("SynchHandler should be of type server");
		}
		
		NioSocketAcceptor socket = new NioSocketAcceptor();
		socket.setHandler(handler);
		socket.setReuseAddress(true);
		List<SocketAddress> addz = new ArrayList<SocketAddress>();
		
		for(Socket s:synchBindings.getSockets()) {
			
			if(s.getIp().equals(Socket.ANY)) {
				
				for(InetAddress ia:NetHelper.getAllAddresses()) {
					
					addz.add(new InetSocketAddress(ia, s.getPort()));
				}
			}
			else {
				
				addz.add(new InetSocketAddress(s.getIp(),s.getPort()));
			}
		}
		
		Set<ClusterAddress> myAddrzForSynch = new HashSet<ClusterAddress>();
		
		for(SocketAddress so:addz) {
			
			myAddrzForSynch.add(new ClusterAddress(((InetSocketAddress)so).getAddress(),((InetSocketAddress)so).getPort()));
		}
		
		if(addz.size() == 0) {
			
			return;
		}
		
		Set<ClusterAddress> myAddrzForBackend = null;
		
		if(backendBindings != null) {
			
			ArrayList<SocketAddress> backendAddz = new ArrayList<SocketAddress>();
			
			myAddrzForBackend = new HashSet<ClusterAddress>();
			
			for(Socket s:backendBindings.getSockets()) {
				
				if(s.getIp().equals(Socket.ANY)) {
					
					for(InetAddress ia:NetHelper.getAllAddresses()) {
						
						backendAddz.add(new InetSocketAddress(ia, s.getPort()));
					}
				}
				else {
					
					backendAddz.add(new InetSocketAddress(s.getIp(),s.getPort()));
				}
			}
			
			for(SocketAddress so:backendAddz) {
				
				myAddrzForBackend.add(new ClusterAddress(((InetSocketAddress)so).getAddress(),((InetSocketAddress)so).getPort()));
			}	
		}
		
		ClusterNode me = synchContext.getMyInfo();
		
		if(!me.isValid()) {
			
			log.error("I was disabled");
			return;
		}
		
		boolean changed = false;
		long lastModified = new Date().getTime();
		
		if(me.getSynchAddresses() == null) {
			
			final Set<Short> awareIds = new HashSet<Short>();
			awareIds.add(synchContext.myId);
			
			
			me.setSynchAddresses(myAddrzForSynch);
			me.setLastModified(lastModified);
			me.setAwareIds(awareIds);
			
			changed = true;
		}
		else if(!me.getSynchAddresses().equals(myAddrzForSynch)) {
			
			final Set<Short> awareIds = new HashSet<Short>();
			awareIds.add(synchContext.myId);
			
			me.setSynchAddresses(myAddrzForSynch);
			me.setLastModified(lastModified);
			me.setAwareIds(awareIds);
			
			changed = true;
		}
		
		if(myAddrzForBackend != null && myAddrzForBackend.size() > 0) {
			
			if(me.getSynchAddresses() == null) {
				
				final Set<Short> awareIds = new HashSet<Short>();
				awareIds.add(synchContext.myId);
				
				
				me.setBackendAddresses(myAddrzForBackend);
				me.setLastModified(lastModified);
				me.setAwareIds(awareIds);
				
				changed = true;
			}
			else if(!me.getSynchAddresses().equals(myAddrzForSynch)) {
				
				final Set<Short> awareIds = new HashSet<Short>();
				awareIds.add(synchContext.myId);
				
				me.setBackendAddresses(myAddrzForBackend);
				me.setLastModified(lastModified);
				me.setAwareIds(awareIds);
				
				changed = true;
			}
		}
		
		if(changed) {
			
			synchContext.updateFrNode(me);
			synchContext.setVirtualLastModified(lastModified);
		}
		
		socket.bind(addz);
		log.debug("FRSYNCH is listning on {} ",addz);
		
		new StartupManager(synchContext).startClusterSynching();
	}
	
	private static class NetHelper {

		private static List<InetAddress> addresses = null;
		
		public static List<InetAddress> getAllAddresses() throws SocketException {
			
			if(addresses != null)
				
				return addresses;
			
			addresses = new ArrayList<InetAddress>();
			
			Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
	        
			for (NetworkInterface netint : Collections.list(nets)) {
				
	        	Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
	        	
	        	for (InetAddress inetAddress : Collections.list(inetAddresses)) {
	        		
	                addresses.add(inetAddress);
	            }
	        }
			
	        if(addresses.size()==0)
	        	
	        	addresses = null;
	        
	        return addresses;
		}
	}
}
