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

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.log.Logger;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.net.InetSocketAddress;

public class Server
{
    private static final Logger log = Logger.get(Server.class);

    private Server()
    {
    }

    public static void main(String[] args)
            throws Exception
    {
        Bootstrap app = new Bootstrap(new DhtServerModule());
        Injector injector = app.strictConfig().initialize();

        int port = args.length > 0 ? Integer.parseInt(args[0]) : 58999;
        EventLoopGroup group = new NioEventLoopGroup();
        final Dht dht = injector.getInstance(Dht.class);

//        Request putReuqst = new Request(Request.Type.SET, "key".getBytes(), "value".getBytes());
//        dht.process(putReuqst);
//        byte[] nums = {4, 5, 6, 7};
//        Request getRequest = new Request(Request.Type.GET_FILTER, "key".getBytes(), nums);
//        dht.process(getRequest);

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(group)
                    .channel(NioServerSocketChannel.class)
                    .localAddress(new InetSocketAddress(port))
                    .childHandler(new ChannelInitializer<SocketChannel>()
                    {
                        @Override
                        protected void initChannel(SocketChannel ch)
                                throws Exception
                        {
                            ch.pipeline().addLast(new LoggingHandler(LogLevel.DEBUG)).addLast(new LengthFieldBasedFrameDecoder(100 * 1024 * 1024, 0, 4)).addLast(new DhtRequestHandler(dht));
                        }
                    });

            ChannelFuture f = b.bind().sync();

            log.info("Server started to listen on port: %d", port);

            f.channel().closeFuture().sync();
        }
        finally {
            group.shutdownGracefully();
        }
    }
}
