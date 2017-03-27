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
package com.facebook.presto.dht.client;

import com.facebook.presto.dht.common.Request;
import com.facebook.presto.dht.common.Response;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LoggingHandler;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Client
        implements ResponseListener
{
    private final Bootstrap bootstrap;
    private final Map<Long, Future<byte[]>> futureMap;
    private Channel channel;

    public Client(SocketAddress remoteAddr)
    {
        this.futureMap = new ConcurrentHashMap<>();

        bootstrap = new Bootstrap();
        EventLoopGroup group = new NioEventLoopGroup();
        final DhtClientHandler clientHandler = new DhtClientHandler(this);
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .remoteAddress(remoteAddr)
                .handler(new ChannelInitializer<SocketChannel>()
                {
                    @Override
                    protected void initChannel(SocketChannel ch)
                            throws Exception
                    {
                        ch.pipeline().addLast(new LoggingHandler()).addLast(new LengthFieldBasedFrameDecoder(100 * 1024 * 1024, 0, 4)).addLast(clientHandler);
                    }
                });
    }

    public static void main(String[] args)
            throws InterruptedException, ExecutionException
    {
        Client client = new Client(new InetSocketAddress("localhost", 1234));
        client.put("key".getBytes(), "values".getBytes());

        Future<byte[]> f = client.get("key".getBytes());

        System.out.print("The value: " + new String(f.get()));
    }

    private Channel getChannel()
    {
        if (channel == null || !channel.isActive()) {
            channel = bootstrap.connect().awaitUninterruptibly().channel();
        }

        return channel;
    }

    public Future<?> put(byte[] key, byte[] value)
    {
        Request putRequest = new Request(Request.Type.SET, key, value);

        return getChannel().writeAndFlush(putRequest.toBuf());
    }

    public Future<byte[]> get(byte[] key)
    {
        Request getRequest = new Request(Request.Type.GET, key, null);

        getChannel().writeAndFlush(getRequest.toBuf());

        Future<byte[]> resultFuture = new CompletableFuture<>();

        futureMap.put(getRequest.getId(), resultFuture);

        return resultFuture;
    }

    @Override
    public void onResponse(Response response)
    {
        Future<byte[]> respFuture = futureMap.get(response.getId());
        if (respFuture != null) {
            CompletableFuture<byte[]> future = (CompletableFuture<byte[]>) respFuture;
            future.complete(response.getPayload());
            futureMap.remove(response.getId());
        }
    }
}
