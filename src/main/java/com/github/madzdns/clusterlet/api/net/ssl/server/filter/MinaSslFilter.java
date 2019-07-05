package com.github.madzdns.clusterlet.api.net.ssl.server.filter;

import java.net.InetSocketAddress;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.core.write.WriteRequest;
import org.apache.mina.filter.ssl.SslFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinaSslFilter extends SslFilter {
	
	private Logger log = LoggerFactory.getLogger(getClass());
	
	public static final String NAME = MinaSslFilter.class.getName()+"ssl_filter";
	
	static final String HANDSHAKE_PARSED = MinaSslFilter.class.getName()+"#$Handed3435";
	
	public final static String HAS_SSL = MinaSslFilter.class.getName()+"HAS_SSL";

	public MinaSslFilter(SSLContext sslContext) {
		
		super(sslContext);
	}
	
	public MinaSslFilter(SSLContext sslContext, boolean auto_start) {
		
		super(sslContext,auto_start);
	}

	@Override
	public void messageReceived(NextFilter next, IoSession session, Object message)
			throws SSLException {
		
		if(session.containsAttribute(IS_NOT_SSL)) {
			
			next.messageReceived(session, message);

			return;
		}
		
		boolean wrongMSG = false;
		
		try {
			
			super.messageReceived(next, session, message);
			
			session.setAttribute(HAS_SSL, Boolean.TRUE);
			
		} catch(Exception e) {
			
			if(wrongMSG) {
				
				//errornous SSL
				throw e;
			}
			
			if(message instanceof IoBuffer) {
				
				
				String clientip = ((InetSocketAddress)session.getRemoteAddress()).getAddress().getHostAddress();
				
				((IoBuffer)message).flip();
				log.warn("Handshake Error, REQUEST SOURCE <{}>, - continue as non ssl message",
						clientip);
				
				next.messageReceived(session, message);
			}
		}
	}
	
	@Override
	public void filterWrite(NextFilter next, IoSession session, WriteRequest write)
			throws SSLException {
		
		if(session.containsAttribute(IS_NOT_SSL)) {
			
			next.filterWrite(session, write);
			
			return;
		}
		
		super.filterWrite(next, session, write);
	}
	
	@Override
	public void sessionClosed(NextFilter nextFilter, IoSession session)
			throws SSLException {
		
		
		super.sessionClosed(nextFilter, session);
	}
	
}
