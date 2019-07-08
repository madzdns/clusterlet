package com.github.madzdns.clusterlet.codec.mina;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;

import lombok.extern.slf4j.Slf4j;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoder;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.madzdns.clusterlet.codec.SynchMessage;
import com.github.madzdns.clusterlet.helper.Types;

@Slf4j
public class SynchMinaEncoder implements ProtocolEncoder {
    @Override
    public void dispose(IoSession session) {
    }

    @Override
    public void encode(IoSession session, Object in, ProtocolEncoderOutput out)
            throws Exception {
        if (!(in instanceof SynchMessage)) {
            return;
        }
        SynchMessage message = (SynchMessage) in;
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try (DataOutputStream os = new DataOutputStream(stream)) {
            message.serialize(os);
            int mlen = os.size();
            if (mlen == Integer.MAX_VALUE) {
                throw new BufferOverflowException();
            }
            IoBuffer bb = IoBuffer.allocate(mlen + Types.ShortBytes);
            bb.putShort((short) mlen);
            bb.put(stream.toByteArray());
            bb.flip();
            out.write(bb);
        }
    }
}
