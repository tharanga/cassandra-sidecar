package org.apache.cassandra.sidecar.cdc;

import java.nio.ByteBuffer;
import org.apache.cassandra.db.partitions.PartitionUpdate;

/**
 * Enum representing the payload type of a Change
 */
enum PayloadType
{
    PARTITION_UPDATE((short) 0),
    MUTATION((short) 1),
    URI((short) 2);

    private short value;

    PayloadType(short value)
    {
        this.value = value;
    }

    public short getValue()
    {
        return this.value;
    }
}

/*
   _________________________________________________________________
    |           |           |           |                           |
    |   Type    |   Version |   Flags   |   Serialized Payload bytes|
    | (2 bytes) | (4 bytes) | (2 bytes) |     (variable length)     |
    |___________|___________|___________|___________________________|

Type
====
The type of the payload. Defined in the PayloadType enum. Example
content types are PartitionUpdate, Mutation, and URI. (short integer)

Version
=======
Some payloads (e.g. PartitionUpdate) need a version for de-serializing .(integer)

Flags
=====
Any custom flags that depends on the use case. E.g. someone can use these to differentiate
between snapshot and change events. (short integer)

Serialized Payload
=====================
Serialized payload. This is just a byte array.

*/

/**
* Defines the envelop of a CDC event
*
 * */
public class Change
{
    static final short CHANGE_EVENT = 0;
    static final short REFRESH_EVENT = 1;
    static final int HEADER_SIZE = 8;
    private short type;

    private int version;

    private short flags;

    private byte[] payload;

    private String partitionKey;



    public Change(PayloadType type, int version, short flags, PartitionUpdate partitionUpdate)
    {
        this.type = type.getValue();
        this.version = version;
        this.flags = flags;
        this.payload = PartitionUpdate.toBytes(partitionUpdate, this.version).array();
        this.partitionKey = partitionUpdate.metadata().partitionKeyType.getSerializer()
                .toCQLLiteral(partitionUpdate.partitionKey().getKey());
    }

    public Change(byte[] serializedChange)
    {
        ByteBuffer buff = ByteBuffer.wrap(serializedChange);
        this.type = buff.getShort(0);
        this.version = buff.getInt(2);
        this.flags = buff.getShort(6);
        this.payload = new byte[serializedChange.length - Change.HEADER_SIZE];
        buff.position(Change.HEADER_SIZE);
        buff.get(this.payload, 0, this.payload.length);
    }

    public byte[] toBytes() throws Exception
    {
        // We don't need to serialize the partition key
        ByteBuffer dob = ByteBuffer.allocate(Change.HEADER_SIZE + this.payload.length);
        dob.putShort(this.type);
        dob.putInt(this.version);
        dob.putShort(this.flags);
        dob.put(this.payload);
        return dob.array();
    }

    public PartitionUpdate getPartitionUpdateObject() throws Exception
    {
        if (this.payload == null || this.type != PayloadType.PARTITION_UPDATE.getValue())
        {
            throw new Exception("Invalid Payload type");
        }
        return PartitionUpdate.fromBytes(ByteBuffer.wrap(this.payload), this.version);
    }

    public String getPartitionKey()
    {
        return this.partitionKey;
    }
}
