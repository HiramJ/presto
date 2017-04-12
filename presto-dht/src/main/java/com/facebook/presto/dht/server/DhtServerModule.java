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

import com.facebook.presto.dht.common.BlockEncodingManager;
import com.facebook.presto.dht.common.Request;
import com.facebook.presto.spi.block.BlockEncodingFactory;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.json.JsonBinder.jsonBinder;

public class DhtServerModule
        implements Module
{
    private static Map<Integer, DhtRequestProcessor> buildProcessorMap(Binder binder)
    {
        Provider<BlockEncodingSerde> blockEncodingSerde = binder.getProvider(BlockEncodingSerde.class);
        Map<ByteBuffer, byte[]> dht = new ConcurrentHashMap<>();
        return ImmutableMap.<Integer, DhtRequestProcessor>builder()
                .put(Request.Type.GET.ordinal(), new GetProcessor(dht))
                .put(Request.Type.SET.ordinal(), new PutProcessor(dht))
                .put(Request.Type.GET_FILTER.ordinal(), new GetFilterProcessor(dht, blockEncodingSerde))
                .put(Request.Type.REMOVE.ordinal(), new RemoveProcessor(dht))
                .build();
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(BlockEncodingManager.class).in(Scopes.SINGLETON);
        binder.bind(BlockEncodingSerde.class).to(BlockEncodingManager.class).in(Scopes.SINGLETON);
        binder.bind(TypeRegistry.class).in(Scopes.SINGLETON);
        binder.bind(TypeManager.class).to(TypeRegistry.class).in(Scopes.SINGLETON);
        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        newSetBinder(binder, Type.class);
        newSetBinder(binder, new TypeLiteral<BlockEncodingFactory<?>>() {});
        binder.bind(new TypeLiteral<Map<Integer, DhtRequestProcessor>>() {}).annotatedWith(Names.named("PROCESSORS")).toInstance(buildProcessorMap(binder));
        binder.bind(Dht.class);
    }
}
