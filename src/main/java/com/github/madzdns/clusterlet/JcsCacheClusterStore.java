package com.github.madzdns.clusterlet;

import org.apache.commons.jcs.engine.CacheElement;
import org.apache.commons.jcs.engine.behavior.ICacheElement;
import org.apache.commons.jcs.engine.control.CompositeCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

public class JcsCacheClusterStore extends JcsCacheStore implements
        IClusterStore {
    private Logger log = LoggerFactory.getLogger(JcsCacheClusterStore.class);
    private CompositeCache<Serializable, Serializable> cache;
    private String cacheName;

    public JcsCacheClusterStore(String jcs_conf_path) throws Exception {
        super(jcs_conf_path);
        cacheName = jcs_conf_path;
        cache = ccm.getCache(cacheName);
    }

    @Override
    public void update(Member node) {
        ICacheElement<Serializable, Serializable> element = new CacheElement<>(cacheName, node.getId(), node);
        try {
            cache.update(element);
        } catch (IOException e) {
            log.error("", e);
        }
    }

    @Override
    public Member get(Short id) {
        ICacheElement<Serializable, Serializable> element = cache.get(id);
        if (element != null) {
            return (Member) element.getVal();
        }
        return null;
    }

    @Override
    public void shutdown() {
        cache.dispose();
    }

    @Override
    public void iterator(IClusterStoreIteratorCallback callbak) {
        for (Serializable serializable : cache.getKeySet()) {
            ICacheElement<Serializable, Serializable> e = cache.get(serializable);
            if (e != null) {
                if (e.getVal() instanceof Member) {
                    callbak.next((Member) e.getVal());
                }
            }
        }
    }
}
