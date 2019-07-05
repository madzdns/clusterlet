package com.github.madzdns.clusterlet.api.net.compress.filter;

import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.compression.CompressionFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinaCompressionFilter extends CompressionFilter {

	public final static String COMPRESS_FILTER = "compression_filter";
	
	private static Logger log = LoggerFactory.getLogger(MinaCompressionFilter.class);
	
	@Override
	public void messageReceived(NextFilter nextFilter, IoSession session,
			Object message) throws Exception {
		try{
			
			super.messageReceived(nextFilter, session, message);
			
			
		}catch(Exception e)
		{
			CompressionFilter compress = (CompressionFilter)session.getFilterChain().get(COMPRESS_FILTER);
			
			if(compress == null)
			{
				
				log.error("Is not this wierd?");
				
				nextFilter.messageReceived(session, message);
				
				return;
				
			}
			
			compress.setCompressInbound(false);
			compress.setCompressOutbound(false);
			nextFilter.messageReceived(session, message);
		}
	}
}
