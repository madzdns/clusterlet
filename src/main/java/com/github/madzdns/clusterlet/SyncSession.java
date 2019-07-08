package com.github.madzdns.clusterlet;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLContext;

import com.github.madzdns.clusterlet.codec.SyncMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.filter.ssl.SslFilter;

import com.github.madzdns.clusterlet.Member.ClusterAddress;
import com.github.madzdns.clusterlet.api.net.NetProvider;
import com.github.madzdns.clusterlet.api.net.compress.filter.MinaCompressionFilter;
import com.github.madzdns.clusterlet.codec.mina.SyncMinaDecoder;
import com.github.madzdns.clusterlet.codec.mina.SyncMinaEncoder;

@Slf4j
class SyncSession {
    class MinaConnectListener implements IoFutureListener<ConnectFuture> {
        private String link = "";
        private SyncMessage msg;

        public MinaConnectListener(SyncMessage msg) {
            this.msg = msg;
        }

        @Override
        public void operationComplete(ConnectFuture connection) {
            Throwable e = connection.getException();
            if (e != null) {
                if (e instanceof ConnectException) {
                    log.error("{} to member {}", e.getMessage(), SyncSession.this.getMemberId());
                } else {
                    log.error("", e);
                }
                if (e instanceof ConnectException)
                    handler.workCallback(SyncSession.this, SyncHandler.STATE_WORK_FAILED, link);
            } else if (!connection.isConnected()) {
                handler.workCallback(SyncSession.this, SyncHandler.STATE_WORK_FAILED, link);

            } else {
                connection.getSession().setAttribute("SyncSession", SyncSession.this);
                log.debug("Connection is stablished in link {} ", link);
                connection.getSession().write(msg);
            }
        }

        public String getLink() {
            return link;
        }

        public void setLink(String remote, int port) {
            this.link = remote + ":" + port;
        }
    }

    private int currentSocket = 0;
    private int lastSocket = 0;
    private List<ClusterAddress> sockets;
    private Object mutx = new Object();
    private SyncHandler handler;
    private Member member;
    private boolean allTried = false;
    private boolean improper = false;
    final Object improperMutex = new Object();

    public SyncSession(Member member, SyncHandler handler) {
        this.sockets = new ArrayList<>(member.getSyncAddresses());
        this.handler = handler;
        this.member = member;

        if (this.sockets.size() == 0) {
            member.setCurrentSocketIndex(currentSocket = -1);
        } else {
            currentSocket = member.getCurrentSocketIndex();
            lastSocket = currentSocket;
        }
    }

    public void setupNextSocket() throws Exception {
        if (currentSocket == -1) {
            log.error("No Socket Containers is defind");
            throw new Exception("No Socket Containers is defind");
        }
        currentSocket = (++currentSocket) % sockets.size();
        ClusterAddress c = sockets.get(currentSocket);
        if (log.isDebugEnabled()) {
            log.debug("Setting next socket {}:{} of edge {}", c.getAddress().getHostAddress(), c.getPort(), member.getId());
        }
        member.setCurrentSocketIndex(currentSocket);
        if (currentSocket == lastSocket) {
            allTried = true;
        }
    }

    public List<ClusterAddress> getSockets() {
        return sockets;
    }

    public void setSockets(List<ClusterAddress> sockets) {
        this.sockets = sockets;
    }

    public void sendMsg(SyncMessage msg) {
        synchronized (mutx) {
            if (currentSocket > -1
                    && handler != null) {
                ClusterAddress currentEdgeSocketAddr = sockets.get(currentSocket);
                SyncSocket socket =
                        new SyncSocket(currentEdgeSocketAddr.getAddress().getHostAddress(),
                                currentEdgeSocketAddr.getPort());
                socket.setHandler(handler);
                if (!member.isAuthByKey()) {
                    log.warn("no need to authenticate by key for member {}", getMemberId());
                    msg.setKeyChain(null);
                } else {
                    msg.setKeyChain(member.getKeyChain());
                }

                if (member.isUseSsl()) {
                    String cer = handler.syncContext.getConfig().getCertificatePath();
                    SSLContext ssl = null;
                    if (cer == null) {
                        ssl = NetProvider.getClientSslContext();
                        log.warn("Could not find any certificate file. Using SSL without verification enable");
                    } else {
                        ssl = NetProvider.getClientSslContext(cer);
                    }

                    if (ssl != null) {
                        SslFilter sslFilter = new SslFilter(ssl);
                        sslFilter.setUseClientMode(true);
                        socket.setFilter("ssl_filter", sslFilter);
                    }
                }

                socket.setFilter("compress_filter",
                        new MinaCompressionFilter());

                socket.setFilter("syncSocket_codec",
                        new SyncMinaEncoder(),
                        new SyncMinaDecoder());
                try {
                    socket.connect(this.new MinaConnectListener(msg));
                } catch (Exception e) {
                    log.error("", e);
                    handler.workCallback(this, SyncHandler.STATE_WORK_FAILED,
                            currentEdgeSocketAddr.getAddress().getHostAddress());
                }
            } else if (handler != null) {
                handler.workCallback(this, SyncHandler.STATE_WORK_FAILED, "");
            }
        }
    }

    public SyncHandler getHandler() {
        return handler;
    }
    public void setHandler(SyncHandler handler) {
        this.handler = handler;
    }
    public boolean isAllTried() {
        return allTried || currentSocket < 0;
    }
    public void setAllTried(boolean allTried) {
        this.allTried = allTried;
    }

    public short getMemberId() {
        if (member == null) {
            return 0;
        }
        return member.getId();
    }

    public Member getMember() {
        return member;
    }

    public boolean isImproper() {
        return improper;
    }

    public void setImproper(boolean improper) {
        this.improper = improper;
    }

    @Override
    public String toString() {
        return String.valueOf(getMemberId());
    }
}
