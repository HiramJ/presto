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

public class RemoveProcessor
        extends AbstractDhtProcessor
{
    private static final Logger log = Logger.get(RemoveProcessor.class);
    private static final byte[] SUCCESS = null;
    private final Map<ByteBuffer, byte[]> localDht;

    @Inject
    public RemoveProcessor(@Named("DHT") Map<ByteBuffer, byte[]> localDht)
    {
        this.localDht = localDht;
    }

    @Override
    protected byte[] getResponse(byte[] key, byte[] value)
    {
        ByteBuffer wrappedKey = ByteBuffer.wrap(key);
        if (localDht.containsKey(wrappedKey)) {
            localDht.remove(wrappedKey);
        }
        else {
            log.info("%s not found in local Dht, nothing removed", new String(key));
        }

        return SUCCESS;
    }

    @Override
    protected boolean sendResponse()
    {
        return false;
    }
}
