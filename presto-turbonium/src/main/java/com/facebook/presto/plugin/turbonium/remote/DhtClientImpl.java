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

import com.facebook.presto.dht.client.Client;
import com.facebook.presto.dht.filters.PageFilter;

import java.net.SocketAddress;
import java.util.concurrent.Future;

public class DhtClientImpl
        implements DhtClient
{
    private final Client client;

    public DhtClientImpl(Client client)
    {
        this.client = client;
    }

    public DhtClientImpl(SocketAddress address)
    {
        this(new Client(address));
    }

    @Override
    public Future<?> put(byte[] key, byte[] value)
    {
        return client.put(key, value);
    }

    @Override
    public Future<byte[]> get(byte[] key)
    {
        return client.get(key);
    }

    @Override
    public Future<byte[]> get(byte[] key, PageFilter filter)
    {
        byte[] fitlerBytes = filter.toBytes();
        return client.get(key, fitlerBytes);
    }
}
