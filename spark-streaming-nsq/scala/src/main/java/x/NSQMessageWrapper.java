package x;

import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQMessage;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import static x.Utils.intToBytes;
import static x.Utils.longToBytes;


// private final byte[] timestamp;
// private final byte[] attempts;
// private final byte[] messageID;
// private final byte[] messageBody;
// private final Address address;
// private final Integer connectionID; // be sure that is not null

public class NSQMessageWrapper implements Serializable{
    transient private NSQMessage message;
    public NSQMessageWrapper(NSQMessage message) {
        this.message = message;
    }

    public NSQMessage getMessage() {
        return message;
    }

    private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException{
        s.defaultReadObject();
        byte[] timestamp, attempts, messageID, messageBody;
        long internalID, traceID, diskQueueOffset;
        int connectionID, diskQueueDataSize, nextConsumingInSecond;
        Address address = new Address("0.0.0.0","0","0", "topic",0);

        timestamp = new byte[s.readInt()];
        s.read(timestamp);

        attempts = new byte[s.readInt()];
        s.read(attempts);

        messageID = new byte[s.readInt()];
        s.read(messageID);

        messageBody = new byte[s.readInt()];
        s.read(messageBody);

        address = (Address) s.readObject();
        connectionID = s.readInt();

        // public NSQMessage(byte[] timestamp, byte[] attempts, byte[] messageID, byte[] messageBody, Address address, Integer connectionID) {
        message = new NSQMessage(timestamp, attempts, messageID, messageBody, address, connectionID);
    }

    private void writeObject(ObjectOutputStream s) throws IOException {
        s.defaultWriteObject();

        s.writeInt(message.getTimestamp().length);
        s.write(message.getTimestamp());

        s.writeInt(message.getAttempts().length);
        s.write(message.getAttempts());

        s.writeInt(message.getMessageID().length);
        s.write(message.getMessageID());

        s.writeInt(message.getMessageBody().length);
        s.write(message.getMessageBody());

        s.writeInt(message.getConnectionID());
    }
}