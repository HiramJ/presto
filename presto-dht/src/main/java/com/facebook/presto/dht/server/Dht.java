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

import com.facebook.presto.dht.common.Request;
import com.facebook.presto.dht.common.Response;
import com.google.common.collect.ImmutableMap;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Dht
        implements DhtRequestProcessor
{
    private final Map<ByteBuffer, byte[]> localDht;
    private final Map<Integer, DhtRequestProcessor> processors;

    public Dht()
    {
        this.localDht = new ConcurrentHashMap<>();
        processors = ImmutableMap.<Integer, DhtRequestProcessor>builder()
                .put(Request.Type.GET.ordinal(), new GetProcessor(localDht))
                .put(Request.Type.SET.ordinal(), new PutProcessor(localDht))
                .build();
    }

    @Override
    public Response process(Request request)
    {
        return processors.get(request.getType().ordinal()).process(request);
    }
}
