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
import io.airlift.log.Logger;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

@ChannelHandler.Sharable
public class DhtRequestHandler
        extends ChannelInboundHandlerAdapter
{
    private static final Logger log = Logger.get(DhtRequestHandler.class);
    private final Dht dht;

    public DhtRequestHandler()
    {
        log.info("Dht handler created");
        dht = new Dht();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
        ByteBuf buf = (ByteBuf) msg;
        Request request = Request.fromBuf(buf);

        Response resp = dht.process(request);

        ctx.writeAndFlush(resp.toBuf());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        log.error(cause, "Error occurred while handling DHT request");
        ctx.close();
    }
}
