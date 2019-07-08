package com.github.madzdns.clusterlet.codec.mina;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

import lombok.extern.slf4j.Slf4j;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.madzdns.clusterlet.codec.SynchMessage;
import com.github.madzdns.clusterlet.helper.Types;

@Slf4j
public class SynchMinaDecoder extends CumulativeProtocolDecoder {
    private static final String DECODE_STATE = "_state." + SynchMinaDecoder.class;

    @Override
    protected boolean doDecode(IoSession session, IoBuffer in,
                               ProtocolDecoderOutput out) throws Exception {
        if (in.remaining() > 0) {
            Short len = (Short) session.getAttribute(DECODE_STATE, (short) -1);
            if (len == -1 && in.remaining() < Types.ShortBytes) {
                return false;
            } else if (len == -1) {
                len = in.getShort();
                session.setAttribute(DECODE_STATE, len);
            }
            if (in.remaining() < len) {
                return false;
            }

            SynchMessage msg = new SynchMessage();
            byte[] data = new byte[len];
            in.get(data);
            try (DataInputStream ins = new DataInputStream(new ByteArrayInputStream(data))) {
                msg.deserialize(ins);
                out.write(msg);
                session.removeAttribute(DECODE_STATE);
                return true;
            }
        }
        return false;
    }
}
