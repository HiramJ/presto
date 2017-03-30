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

import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.facebook.presto.plugin.turbonium.TurboniumErrorCode.MISSING_DATA;

public class DhtPageStore
        implements PageStore
{
    private static final Logger log = Logger.get(DhtPageStore.class);
    private final Map<Long, TableInfo> tableInfos;
    private final DhtClientProvider clientProvider;
    private final BlockEncodingSerde encodingManager;
    private final byte[] nodeId;

    @Inject
    public DhtPageStore(DhtClientProvider clientProvider, BlockEncodingSerde encodingManager, NodeManager nodeManager)
    {
        this.clientProvider = clientProvider;
        this.encodingManager = encodingManager;
        nodeId = nodeManager.getCurrentNode().getNodeIdentifier().getBytes();
        tableInfos = new ConcurrentHashMap<>();
    }

    private static Page getColumns(Page page, List<Integer> columnIndexes)
    {
        Block[] blocks = page.getBlocks();
        Block[] outputBlocks = new Block[columnIndexes.size()];

        for (int i = 0; i < columnIndexes.size(); i++) {
            outputBlocks[i] = blocks[columnIndexes.get(i)];
        }

        return new Page(page.getPositionCount(), outputBlocks);
    }

    private byte[] getLocalId()
    {
        return nodeId;
    }

    @Override
    public void initialize(String tableName, long tableId)
    {
        if (!tableInfos.containsKey(tableId)) {
            TableInfo tableInfo = new TableInfo(tableId, tableName);
            tableInfos.put(tableId, tableInfo);
        }
    }

    @Override
    public void add(Long tableId, Page page)
    {
        page.compact();
        TableInfo tableInfo = tableInfos.get(tableId);
        PageKey key = new PageKey(tableId, getLocalId(), tableInfo.addPage(page));

        DhtClient client = clientProvider.getClient(key.hashCode());

        byte[] valBytes = getPageBytes(page);

        client.put(key.getBytes(), valBytes);
    }

    private byte[] getPageBytes(Page page)
    {
        SliceOutput output = new DynamicSliceOutput((int) page.getSizeInBytes());
        PageUtil.writeRawPage(page, output, encodingManager);

        Slice slice = output.getUnderlyingSlice();
        return slice.getBytes();
    }

    @Override
    public List<Page> getPages(Long tableId, int partNumber, int totalParts, List<Integer> columnIndexes)
    {
        TableInfo info = tableInfos.get(tableId);
        if (info == null) {
            throw new PrestoException(MISSING_DATA, "Failed to find table on a worker.");
        }
        long pageCount = info.getPageCount();
        List<Future<byte[]>> pageFutures = new ArrayList<>();
        for (int i = partNumber; i < pageCount; i += totalParts) {
            PageKey key = new PageKey(tableId, getLocalId(), i);
            DhtClient client = clientProvider.getClient(key.hashCode());
            pageFutures.add(client.get(key.getBytes()));
        }

        final int totalPages = pageFutures.size();
        log.info("Wait to get back all the %d pages", totalPages);
        final AtomicInteger gotPageCount = new AtomicInteger(0);

        List<Page> pages = pageFutures.stream().map(f -> {
            try {
                byte[] pageBytes = f.get(60, TimeUnit.MINUTES);
                log.info("Got %d of %d pages back...", gotPageCount.incrementAndGet(), totalPages);
                Slice slice = Slices.wrappedBuffer(pageBytes, 0, pageBytes.length);

                return PageUtil.readRawPage(slice.getInput(), encodingManager);
            }
            catch (Throwable e) {
                log.error(e, "Failed to get page");
                return null;
            }
        }).map(p -> getColumns(p, columnIndexes)).collect(Collectors.toList());

        return pages;
    }

    @Override
    public List<Long> listTableIds()
    {
        return ImmutableList.copyOf(tableInfos.keySet());
    }

    @Override
    public SizeInfo getSize(Long tableId)
    {
        return tableInfos.get(tableId).getSizeInfo();
    }

    @Override
    public boolean contains(Long tableId)
    {
        return tableInfos.containsKey(tableId);
    }

    @Override
    public void cleanUp(Set<Long> activeTableIds)
    {
        // todo: add remove requests
    }

    private static class TableInfo
    {
        private final long id;
        private final String tableName;
        private final SizeInfo sizeInfo;
        private long pageCount;

        TableInfo(long id, String tableName)
        {
            this.id = id;
            this.tableName = tableName;
            this.pageCount = 0;
            this.sizeInfo = new SizeInfo(0, 0);
        }

        synchronized long addPage(Page page)
        {
            sizeInfo.update(page.getPositionCount(), page.getRetainedSizeInBytes());
            return pageCount++;
        }

        SizeInfo getSizeInfo()
        {
            return sizeInfo;
        }

        long getPageCount()
        {
            return pageCount;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            return String.format("id: %s, tableName: %s, pageCount: %s, sizeInfo: %s", id, tableName, pageCount, sizeInfo);
        }
    }
}
