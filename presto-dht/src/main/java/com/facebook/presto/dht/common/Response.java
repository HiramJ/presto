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

public class Response
{
    private final long id;

    private final byte[] payload;

    public Response(long id, byte[] payload)
    {
        this.id = id;
        this.payload = payload;
    }

    public static Response fromBuf(ByteBuf buf)
    {
        int length = buf.readInt();
        long id = buf.readLong();
        int payloadSize = buf.readInt();

        byte[] payLoad = null;
        if (payloadSize > 0) {
            payLoad = new byte[payloadSize];
            buf.readBytes(payLoad, 0, payloadSize);
        }

        return new Response(id, payLoad);
    }

    public long getId()
    {
        return id;
    }

    public byte[] getPayload()
    {
        return payload;
    }

    public ByteBuf toBuf()
    {
        int length = Long.BYTES +
                Integer.BYTES +
                (payload == null ? 0 : payload.length);
        ByteBuf buf = Unpooled.buffer(Integer.BYTES + length);
        buf.writeInt(length);
        buf.writeLong(id);
        if (payload == null) {
            buf.writeInt(0);
        }
        else {
            buf.writeInt(payload.length);
            buf.writeBytes(payload);
        }

        return buf;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("id: " + id);
        sb.append(" payload: " + (payload == null ? null : new String(payload)));
        return sb.toString();
    }
}
