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

import com.google.inject.name.Named;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.nio.ByteBuffer;
import java.util.Map;

public class GetProcessor
        extends AbstractDhtProcessor
{
    private static final Logger log = Logger.get(GetProcessor.class);
    private final Map<ByteBuffer, byte[]> localDht;

    @Inject
    public GetProcessor(@Named("DHT") Map<ByteBuffer, byte[]> localDht)
    {
        this.localDht = localDht;
    }

    @Override
    protected byte[] getResponse(byte[] key, byte[] value)
    {
        byte[] resp = localDht.get(ByteBuffer.wrap(key));
        if (resp == null) {
            log.warn("Failed to find value on local Dht for key %s", new String(key));
        }
        return resp;
    }

    @Override
    protected boolean sendResponse()
    {
        return true;
    }
}
