# What is Clusterlet?

Clusterlet is a `Java` library to let you manage A to Z of a `cluster`

It helps you to discover other cluster members, Add or remove members, detect dead or unhealthy members and so on.

It also helps you to communicate between cluster members with sending messages.

It keeps track of messages and is capable of resolving conflicts between message versions

It also gives reports related to the state and health of the cluster.

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
SynchConfig config = new SynchConfig(clusterFile,
        sslKeyStorePath, sslTrustStorepath,
        KEYSTORE_PASSWORD, TRUSTSTORE_PASSWORD, KEYSTORE_PASSWORD_2ND,
        CERTIFICATE_PATH);
SynchContext context = new SynchContext(MY_OWN_ID, config);
//Return the created context as a bean or keep it in a global static place
```

### Adding a new member

```java
final short memberId = 2;
final Set<Socket> syncAddresses = Collections.singleton(new Socket("localhost:12346"));
final boolean useSsl = true;
final boolean authByKey = true;
final String key = "member key";
final long lastModified = new Date().getTime();
final Set<Short> awareIds = null;//This new member is not aware of other nodes
final byte state = Member.STATE_VLD;//To delete use Member.STATE_DEL
Member member = new Member(memberId, syncAddresses, useSsl, authByKey, key, lastModified, awareIds, state);

context.synchCluster(member, SynchType.RING);
```

### Synchronization types

In above example, we've used the `SynchType.RING` type to synch the new member with the cluster.
This parameter makes an special hint for the synchronization method.
Different synchronization types are:

1. `UNICAST` sends message to the specified members
2. `UNICAST_BALANCE`  
