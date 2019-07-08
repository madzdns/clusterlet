package com.github.madzdns.clusterlet;

import com.github.madzdns.clusterlet.codec.IMessage;

import java.util.ArrayList;
import java.util.List;

public class SyncProtocolOutput implements ISyncProtocolOutput {
    private List<IMessage> messages = null;

    SyncProtocolOutput() {
    }

    List<IMessage> getMessages() {
        return messages;
    }

    @Override
    public void write(IMessage message) {
        if (message == null) {
            messages = null;
            return;
        }
        this.messages = new ArrayList<>();
        this.messages.add(message);
    }

    @Override
    public void write(List<IMessage> messages) {
        this.messages = messages;
    }
}
