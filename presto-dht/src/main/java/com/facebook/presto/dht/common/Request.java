/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.dht.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.concurrent.atomic.AtomicLong;

public class Request
{
    private static final AtomicLong idGenerator = new AtomicLong(0);
    private final long id;
    private final Type type;
    private final byte[] key;
    private final byte[] value;

    public Request(Type type, byte[] key, byte[] value)
    {
        this.id = idGenerator.incrementAndGet();
        this.type = type;
        this.key = key;
        this.value = value;
    }

    private Request(long id, Type type, byte[] key, byte[] value)
    {
        this.id = id;
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public static Request fromBuf(ByteBuf buf)
    {
        int length = buf.readInt();
        long id = buf.readLong();
        short typeVal = buf.readShort();
        int keyLen = buf.readInt();
        byte[] key = new byte[keyLen];
        buf.readBytes(key, 0, keyLen);
        int valLen = buf.readInt();
        byte[] value = null;
        if (valLen > 0) {
            value = new byte[valLen];
            buf.readBytes(value, 0, valLen);
        }

        return new Request(id, Type.fromShort(typeVal), key, value);
    }

    public long getId()
    {
        return id;
    }

    public Type getType()
    {
        return type;
    }

    public byte[] getKey()
    {
        return key;
    }

    public byte[] getValue()
    {
        return value;
    }

    public ByteBuf toBuf()
    {
        int length = Long.BYTES +
                Short.BYTES +
                Integer.BYTES +
                key.length +
                Integer.BYTES +
                (value == null ? 0 : value.length);
        ByteBuf buf = Unpooled.buffer(Integer.BYTES + length);
        buf.writeInt(length);
        buf.writeLong(id);
        buf.writeShort((short) type.ordinal());
        buf.writeInt(key.length);
        buf.writeBytes(key);
        if (value == null) {
            buf.writeInt(0);
        }
        else {
            buf.writeInt(value.length);
            buf.writeBytes(value);
        }

        return buf;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(" id: " + id);
        sb.append(" type: " + type);
        sb.append(" key: " + new String(key));
        sb.append(" value: " + (value == null ? null : new String(value)));

        return sb.toString();
    }

    public enum Type
    {
        GET,
        SET,
        REMOVE;

        public static Type fromShort(short val)
        {
            switch (val) {
                case 0:
                    return GET;
                case 1:
                    return SET;
                case 2:
                    return REMOVE;
            }

            throw new IllegalArgumentException("Unknown type val: " + val);
        }
    }
}
