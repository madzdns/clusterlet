package com.github.madzdns.clusterlet;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.mina.core.filterchain.IoFilter;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.core.future.WriteFuture;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.ProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolEncoder;
import org.apache.mina.transport.socket.nio.NioSocketConnector;

import com.github.madzdns.clusterlet.SynchSession.MinaConnectListener;

public class SynchSocket {
	
	private NioSocketConnector socket = null;
	
	private IoSession session = null;
	
	private String remote=null;
	
	private String local=null;
	
	private int port=0;
	
	IoFutureListener<ConnectFuture> listener;
	
	
	/**
	 * 
	 * @param listener a value of null, indicates non blocking connect
	 * @throws Exception
	 */
	public void connect(IoFutureListener<ConnectFuture> listener) throws Exception {
		
		this.listener = listener;
		
		if(session == null || !session.isConnected()) {
			
			ConnectFuture cf = null;
			
			if(local == null)
				
				cf = socket.connect(new InetSocketAddress(remote, port));
			
			else
				
				cf = socket.connect(new InetSocketAddress(remote, port),new InetSocketAddress(local,0));
			
			if(cf == null)
				
				throw new IOException("Could not initiate connect to "+remote+":"+port+" through "+local);
			
			if(listener == null) {	
				
				cf.await();
				
				session = cf.getSession();
			}
			else {
				
				cf.addListener(listener);
				
				if(listener instanceof MinaConnectListener) {
					
					((MinaConnectListener)listener).setLink(remote,port);
				}
				
			}
			
			if(cf.getException()!=null)
				
				throw new Exception(cf.getException().getMessage());
			
		}
	}
	
	public SynchSocket(String remote,int port) {
		
		this.remote = remote;
		
		this.port = port;
		
		socket = new NioSocketConnector();
	}
	
	public SynchSocket(String remote,String local,int port) {
		
		this.remote = remote;
		this.local = local;
		this.port = port;
		socket = new NioSocketConnector();
	}
	
	public void setHandler(IoHandler handler) {
		
		socket.setHandler(handler);
	}
	
	public void setFilter(String name,ProtocolEncoder encoder,ProtocolDecoder decoder) {
		
		socket.getFilterChain().addLast(name, new ProtocolCodecFilter(encoder, decoder));
	}
	
	public void setFilter(String name,IoFilter filter) {
		
		socket.getFilterChain().addLast(name,filter);
	}
	
	public IoSession send(Object message) throws Exception {
		
		if(session == null||
				!session.isConnected())
			
			connect(listener);
		
		WriteFuture wf = session.write(message);
		
		if(wf != null) {
			
			Throwable th = wf.getException();
			
			if(th!=null)
				throw new Exception(th.getMessage()); 
		}
		
		return this.session;
	}
	
	public void close(boolean method) {
		
		if(session != null)
			
			session.close(method);
	}
	
	public String getRemote() {
		
		return remote;
	}
	
	public void setRemote(String remote) {
		
		this.remote = remote;
	}
	
	public String getLocal() {
		
		return local;
	}
	
	public void setLocal(String local) {
		
		this.local = local;
	}
	
	public int getPort() {
		
		return port;
	}
	
	public void setPort(int port) {
		
		this.port = port;
	}
}
