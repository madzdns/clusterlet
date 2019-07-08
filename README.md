# What is Clusterlet?

Clusterlet is a `Java` library. It let you manage A to Z of a `cluster`

* It helps you to discover other cluster members, add or remove members, detect dead or unhealthy members and so on.
* It also helps you to communicate between cluster members with sending (bulk) messages.
* It keeps track of messages and is capable of resolving conflicts between message versions
* It also gives reports related to the state and health of the cluster.
* It helps to change credentials or binding address of cluster member on demand without a need shutdown cluster.
* It lets you to send messages in multiple methods depending on your use case (`UNICAST`, `RING`, ...)
* In any given time, It is aware of all the members and all the messages they're have been reached.
* It identifies identical messages along with their versions so would not waste distributing messages to the member who already has that message
* It handles cluster startup for failing or restarted members
* It is a library so can simply be embedded in any java application
  
So... Bring up you're cluster, discover the members and start sending messages    

[MadzDNS cluster](https://github.com/madzdns/cluster) is a proof of concept for this library

## Examples

### Creating a synchronization context

```java
final String sslKeyStorePath = null;
final String sslTrustStorepath = null;
final String KEYSTORE_PASSWORD = null;
final String TRUSTSTORE_PASSWORD = null;
final String KEYSTORE_PASSWORD_2ND = null;
final String CERTIFICATE_PATH = null;
final short MY_OWN_ID = 1;//Id of this member
final String clusterFile = "cluster_file";
new File(clusterFile).createNewFile();
SyncConfig config = new SyncConfig(clusterFile,
        sslKeyStorePath, sslTrustStorepath,
        KEYSTORE_PASSWORD, TRUSTSTORE_PASSWORD, KEYSTORE_PASSWORD_2ND,
        CERTIFICATE_PATH);
SyncContext context = new SyncContext(MY_OWN_ID, config);
//Return the created context as a bean or keep it in a global static place
```

### Creating a cluster listener
```java
SyncHandler handler = context.make()
                    .withCallBack(new SyncCallback(null))
                    .withEncoder(MyMessage.class);
SocketBindConfig syncBinding = new SocketBindConfig();
syncBinding.setSocketConfigs(Collections.singletonList(new SocketConfig("localhost:12346")));
try {
    new SyncServer(handler, syncBinding).start();
} catch (IOException e) {
    fail();
}
```
Above, `SyncCallback` is an implementation of the `ISyncCallback` interface :

```java
public interface ISyncCallback {
    boolean callBack(ISession session, IMessage message,
                     Set<Short> withNodes, ISyncProtocolOutput out);

    void result(SyncFuture syncFeature);
}
```
Clusterlet calls the `callBack` method for every receiving message
and acts based on the return value. Updates its indexing database and informs the 
sending member about the result. It is obvious that a value of `true` means ok and
`false` mean failure. Following is the simplest `ISyncCallback` implementation ever!

```java
private static class SyncCallback implements ISyncCallback {
    @Override
    public boolean callBack(ISession session, IMessage message,
                            Set<Short> withNodes, ISyncProtocolOutput out) {
        return true;
    }

    @Override
    public void result(SyncFeature syncFeature) {

    }
}
```
`result` method will be called for a sender and informs the result the operation
per each message. Here we are explaining about the server and will come to the sender
part in a few seconds.

`MyMessage` is a class that implements `IMessage` below:

```java
public interface IMessage extends IEncoder, IDecoder {
	String getKey();
	long getVersion();
}
```
For Every message `equals` and `hashCode` methods should be defined and make sure
to have a default constructor with no arguments.
Each message is identified with the `key` value. For messages with identical `key`
values, version becomes important.

Following is a simple message implementation:

```java
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class MyMessage implements IMessage {
    private String key;
    private long version;
    private String msg;

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> config) {

    }

    @Override
    public void deserialize(byte[] data) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(data))) {
            key = in.readUTF();
            version = in.readLong();
            msg = in.readUTF();
        } catch (Exception e) {
            log.error("", e);
        }
    }

    @Override
    public byte[] serialize() {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            DataOutputStream out = new DataOutputStream(stream);
            out.writeUTF(getKey());
            out.writeLong(getVersion());
            out.writeUTF(msg);
            return stream.toByteArray();
        } catch (Exception e) {
            log.error("{}, {}", getKey(), getVersion(), e);
            return null;
        }
    }
}
```

### Adding a new member to the cluster

```java
final short memberId = 2;
final Set<Member.ClusterAddress> syncAddresses = Collections.singleton(
        new Member.ClusterAddress("localhost", 12347));
final boolean useSsl = false;
final boolean authByKey = true;
final String key = "";
final long lastModified = new Date().getTime();
final Set<Short> awareIds = null;//This new member is not aware of other nodes
final byte state = Member.STATE_VLD;//To delete use Member.STATE_DEL
Member member = new Member(memberId, syncAddresses, useSsl, authByKey, key, lastModified, awareIds, state);
context.syncCluster(member, SyncType.RING);
```

### Synchronization types

In above example, we've used the `SynchType.RING` type to sync the new member with the cluster.
This parameter makes an special hint for the synchronization method.
Different synchronization types are:

1. `UNICAST` Sends messages to each of the mentioned members or to all members in unicast form. 
2. `RING`  Sends messages one mentioned or all members in ring form, eg. It sends messages to one member and the receiving member sends them to other other node until eligible members all receive the messages.
3. `UNICAST_ONE_OF` Sends messages to one of the members in unicast form.
4. `UNICAST_BALANCE` Sends messages to each of the mentioned members in unicast form only if the members are not previously received messages.
5. `RING_BALANCE` Sends messages in ring form only if the members are not previously received messages.
6. `UNICAST_QUORUM` Sends messages in unicast form and report successful only if the quorum number of members received the messages.
7. `RING_QUORUM` Sends messages in ring form and report successful only if the quorum number of members received the messages.
8. `UNICAST_BALANCE_QUORUM` Sends messages in `UNICAST_BALANCE` format and report successful only if the quorum number of members received the messages.
9. `RING_BALANCE_QUORUM` Sends messages in `RING_BALANCE` format and report successful only if the quorum number of members received the messages.

### Sending a message to the members

```java
MyMessage messageFromMember2 = new MyMessage("myKey", new Date().getTime(), "message body");
SyncFeature feature = context.make(SyncType.RING)
        .withoutCluster(2)//Dont send to member 2 again
        .withCallBack(new SyncCallback())
        .withEncoder(MyMessage.class)
        .sync(messageFromMember2)
        .get();
```
In the above example, I want to send a single message from a member with `id` 2.
I manually excluded member `2` from the sync process so the message would not 
come back to current node again (In some scenario one might want it to do otherwise)
I also decided to send the message in `RING` mode to somehow load the balance between 
members.

The result can be accessed in two form. As mentioned earlier, using the `result` method of
`ISyncCallback` or by calling `get` method of created handler which blocks until gets the response.
The result can be examined using the returned `SyncFeature`:
```java
assertTrue(feature.get("myKey").isSuccessful())
```
Above, we check to see wat happened to the message with key `myKey`

I hope you use and enjoy the library.

<b>Is there anything left? Don't hesitate to create an issue!</b>