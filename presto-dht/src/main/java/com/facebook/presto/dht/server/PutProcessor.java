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
package com.facebook.presto.dht.server;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.airlift.log.Logger;

import java.nio.ByteBuffer;
import java.util.Map;

public class PutProcessor
        extends AbstractDhtProcessor
{
    private static final Logger log = Logger.get(PutProcessor.class);
    private static final byte[] SUCCESS = null;
    private final Map<ByteBuffer, byte[]> localDht;

    @Inject
    public PutProcessor(@Named("DHT") Map<ByteBuffer, byte[]> localDht)
    {
        this.localDht = localDht;
    }

    @Override
    protected byte[] getResponse(byte[] key, byte[] value)
    {
        log.info("Got new record with size: %d", value.length);
        if (localDht.size() % 1000 == 0) {
            long totalValue = localDht.values().stream().mapToLong(d -> d.length).sum();
            log.info("Current total size is %d Bytes", totalValue);
        }

        localDht.put(ByteBuffer.wrap(key), value);
        return SUCCESS;
    }

    @Override
    protected boolean sendResponse()
    {
        return false;
    }
}
