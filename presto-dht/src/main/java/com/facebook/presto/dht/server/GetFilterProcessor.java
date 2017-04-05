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

import com.facebook.presto.dht.common.PageUtil;
import com.facebook.presto.dht.filters.ColumnBasedFilter;
import com.facebook.presto.dht.filters.PageFilter;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.ByteBuffer;
import java.util.Map;

public class GetFilterProcessor
        extends GetProcessor
{
    private static final Logger log = Logger.get(GetFilterProcessor.class);

    private final Provider<BlockEncodingSerde> blockEncodingSerde;

    @Inject
    public GetFilterProcessor(@Named("DHT") Map<ByteBuffer, byte[]> localDht, Provider<BlockEncodingSerde> blockEncodingSerde)
    {
        super(localDht);
        this.blockEncodingSerde = blockEncodingSerde;
    }

    @Override
    protected byte[] getResponse(byte[] key, byte[] value)
    {
        byte[] pageBytes = super.getResponse(key, value);

        if (value == null) {
            log.warn("Filter is null");
        }

        if (pageBytes != null) {
            log.info("%s before filter is in size: %s", key, pageBytes.length);
            PageFilter filter = new ColumnBasedFilter(value);
            Slice slice = Slices.wrappedBuffer(pageBytes, 0, pageBytes.length);
            Page page = PageUtil.readRawPage(slice.getInput(), blockEncodingSerde.get());
            page = filter.filter(page);
            pageBytes = PageUtil.getPageBytes(page, blockEncodingSerde.get());
            log.info("%s after filter is in size: %s", key, pageBytes.length);
        }

        return pageBytes;
    }

    @Override
    protected boolean sendResponse()
    {
        return true;
    }
}
