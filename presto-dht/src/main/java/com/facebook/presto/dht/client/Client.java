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
import io.airlift.log.Logger;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
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
    private static final Logger log = Logger.get(Client.class);
    private final Bootstrap bootstrap;
    private final Map<Long, Future<byte[]>> futureMap;
    private final SocketAddress remoteAddr;
    private Channel channel;

    public Client(SocketAddress remoteAddr)
    {
        this.remoteAddr = remoteAddr;
        this.futureMap = new ConcurrentHashMap<>();

        bootstrap = new Bootstrap();
        EventLoopGroup group = new NioEventLoopGroup();
        final ResponseListener listener = this;
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .remoteAddress(remoteAddr)
                .handler(new ChannelInitializer<SocketChannel>()
                {
                    @Override
                    protected void initChannel(SocketChannel ch)
                            throws Exception
                    {
                        ch.pipeline().addLast(new LoggingHandler(LogLevel.DEBUG)).addLast(new LengthFieldBasedFrameDecoder(100 * 1024 * 1024, 0, 4)).addLast(new DhtClientHandler(listener));
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
            log.info("Building chanel to %s", remoteAddr);
            ChannelFuture channelFuture = bootstrap.connect();
            channel = channelFuture.awaitUninterruptibly().channel();
            if (channelFuture.cause() != null) {
                log.error(channelFuture.cause(), "Error happened when building the channel to " + remoteAddr.toString());
            }
            if (channel.remoteAddress() == null) {
                log.warn("Remote address is null, expected: %s", remoteAddr);
                log.info("The channel future is: %s", channelFuture);
            }
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
        return get(getRequest);
    }

    public Future<byte[]> get(byte[] key, byte[] filter)
    {
        Request filterRequest = new Request(Request.Type.GET_FILTER, key, filter);
        return get(filterRequest);
    }

    private Future<byte[]> get(Request getRequest)
    {
        log.info("Send request id: %d to %s and wait for response", getRequest.getId(), getChannel().remoteAddress());

        Future<byte[]> resultFuture = new CompletableFuture<>();
        futureMap.put(getRequest.getId(), resultFuture);

        getChannel().writeAndFlush(getRequest.toBuf());

        return resultFuture;
    }

    @Override
    public void onResponse(Response response)
    {
        Future<byte[]> respFuture = futureMap.get(response.getId());
        log.info("Received new response of id: %s", response.getId());

        if (respFuture != null) {
            CompletableFuture<byte[]> future = (CompletableFuture<byte[]>) respFuture;
            future.complete(response.getPayload());
            futureMap.remove(response.getId());
        }
        else {
            log.warn("Couldn't find mapping future of response id: %d", response.getId());
        }
    }
}
