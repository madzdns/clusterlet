package com.github.madzdns.clusterlet.codec;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface ISyncMessage {
    void serialize(DataOutputStream out) throws IOException;

    void deserialize(DataInputStream in) throws IOException;
}
