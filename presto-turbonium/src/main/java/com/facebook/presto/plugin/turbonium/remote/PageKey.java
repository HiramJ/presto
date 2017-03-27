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
package com.facebook.presto.plugin.turbonium.remote;

import io.airlift.log.Logger;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;

import java.nio.ByteBuffer;

public class PageKey
{
    private static final Logger log = Logger.get(PageKey.class);
    private final ByteBuffer key;

    public PageKey(byte[] key)
    {
        this.key = ByteBuffer.wrap(key);
    }

    public PageKey(long tableId, byte[] localNodeId, long pageId)
    {
        SliceOutput slice = new DynamicSliceOutput(Long.BYTES + localNodeId.length + Long.BYTES);
        slice.writeLong(tableId);
        slice.writeBytes(localNodeId);
        slice.writeLong(pageId);

        this.key = ByteBuffer.wrap(slice.getUnderlyingSlice().getBytes());
    }

    public byte[] getBytes()
    {
        return key.array();
    }

    @Override
    public boolean equals(Object other)
    {
        return key.equals(((PageKey) other).key);
    }

    @Override
    public int hashCode()
    {
        return key.hashCode();
    }
}
