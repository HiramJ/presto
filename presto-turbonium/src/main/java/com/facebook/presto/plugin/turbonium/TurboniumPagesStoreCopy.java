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
package com.facebook.presto.plugin.turbonium;

import com.facebook.presto.plugin.turbonium.config.TurboniumConfigManager;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.plugin.turbonium.TurboniumErrorCode.MEMORY_LIMIT_EXCEEDED;
import static com.facebook.presto.plugin.turbonium.TurboniumErrorCode.MISSING_DATA;
import static com.facebook.presto.plugin.turbonium.TurboniumErrorCode.TABLE_SIZE_PER_NODE_EXCEEDED;
import static java.lang.String.format;

@ThreadSafe
public class TurboniumPagesStoreCopy
{
    private static final Logger log = Logger.get(TurboniumPagesStore.class);
    private final TurboniumConfigManager configManager;
    private final BlockEncodingSerde encodingManager;
    @GuardedBy("this")
    private final Map<Long, List<Page>> pages = new HashMap<>();
    @GuardedBy("this")
    private final Map<Long, Long> tableSizes = new HashMap<>();
    @GuardedBy("this")
    private long currentBytes = 0;

    @Inject
    public TurboniumPagesStoreCopy(TurboniumConfigManager configManager, BlockEncodingSerde encodingManager)
    {
        this.configManager = configManager;
        this.encodingManager = encodingManager;
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

    static void writeRawPage(Page page, SliceOutput output, BlockEncodingSerde serde)
    {
        Block[] blocks = page.getBlocks();
        output.writeInt(blocks.length);
        for (Block block : blocks) {
            writeBlock(serde, output, block);
        }
    }

    static Page readRawPage(int positionCount, SliceInput input, BlockEncodingSerde blockEncodingSerde)
    {
        int numberOfBlocks = input.readInt();
        Block[] blocks = new Block[numberOfBlocks];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = readBlock(blockEncodingSerde, input);
        }

        return new Page(positionCount, blocks);
    }

    public static Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput input)
    {
        BlockEncoding blockEncoding = blockEncodingSerde.readBlockEncoding(input);
        return blockEncoding.readBlock(input);
    }

    public static void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput output, Block block)
    {
        BlockEncoding encoding = block.getEncoding();
        blockEncodingSerde.writeBlockEncoding(output, encoding);
        encoding.writeBlock(output, block);
    }

    private long getMaxBytes()
    {
        return configManager.getConfig().getMaxDataPerNode().toBytes();
    }

    private long getMaxBytesPerTable()
    {
        return configManager.getConfig().getMaxTableSizePerNode().toBytes();
    }

    public synchronized void initialize(String tableName, long tableId)
    {
        if (!pages.containsKey(tableId)) {
            pages.put(tableId, new ArrayList<>());
            tableSizes.put(tableId, 0L);
        }
    }

    public synchronized void add(Long tableId, Page page)
    {
        if (!contains(tableId)) {
            throw new PrestoException(MISSING_DATA, "Failed to find table on a worker.");
        }
        page.compact();
        long newSize = currentBytes + page.getRetainedSizeInBytes();
        long newTableSize = tableSizes.get(tableId) + page.getRetainedSizeInBytes();
        long maxBytes = getMaxBytes();
        if (maxBytes < newSize) {
            throw new PrestoException(MEMORY_LIMIT_EXCEEDED, format("Memory limit [%d] for memory connector exceeded", maxBytes));
        }
        long maxBytesPerTable = getMaxBytesPerTable();
        if (maxBytesPerTable < newTableSize) {
            throw new PrestoException(TABLE_SIZE_PER_NODE_EXCEEDED, format("Table size [%d] bytes per node exceeded", maxBytesPerTable));
        }
        currentBytes = newSize;
        tableSizes.put(tableId, newSize);
        List<Page> tablePages = pages.get(tableId);
        tablePages.add(page);

        SliceOutput output = new DynamicSliceOutput((int) page.getSizeInBytes());
        writeRawPage(page, output, encodingManager);

        Slice slice = output.getUnderlyingSlice();
        byte[] bytesBuf = slice.getBytes();
        log.error("New page added as bytes size: %d", bytesBuf.length);
    }

    public synchronized List<Page> getPages(Long tableId, int partNumber, int totalParts, List<Integer> columnIndexes)
    {
        if (!contains(tableId)) {
            throw new PrestoException(MISSING_DATA, "Failed to find table on a worker.");
        }

        List<Page> tablePages = pages.get(tableId);
        ImmutableList.Builder<Page> partitionedPages = ImmutableList.builder();

        for (int i = partNumber; i < tablePages.size(); i += totalParts) {
            partitionedPages.add(getColumns(tablePages.get(i), columnIndexes));
        }
        return partitionedPages.build();
    }

    public synchronized List<Long> listTableIds()
    {
        return ImmutableList.copyOf(pages.keySet());
    }

    public synchronized SizeInfo getSize(Long tableId)
    {
        long sizeBytes = pages.get(tableId).stream().mapToLong(Page::getRetainedSizeInBytes).sum();
        long rowCount = pages.get(tableId).stream().mapToLong(Page::getPositionCount).sum();
        return new SizeInfo(rowCount, sizeBytes);
    }

    public synchronized boolean contains(Long tableId)
    {
        return pages.containsKey(tableId);
    }

    public synchronized void cleanUp(Set<Long> activeTableIds)
    {
        // We have to remember that there might be some race conditions when there are two tables created at once.
        // That can lead to a situation when TurboniumPagesStore already knows about a newer second table on some worker
        // but cleanUp is triggered by insert from older first table, which TurboniumTableHandle was created before
        // second table creation. Thus activeTableIds can have missing latest ids and we can only clean up tables
        // that:
        // - have smaller value then max(activeTableIds).
        // - are missing from activeTableIds set

        if (activeTableIds.isEmpty()) {
            // if activeTableIds is empty, we can not determine latestTableId...
            return;
        }
        long latestTableId = Collections.max(activeTableIds);

        for (Iterator<Map.Entry<Long, List<Page>>> tablePages = pages.entrySet().iterator(); tablePages.hasNext(); ) {
            Map.Entry<Long, List<Page>> tablePagesEntry = tablePages.next();
            Long tableId = tablePagesEntry.getKey();
            if (tableId < latestTableId && !activeTableIds.contains(tableId)) {
                for (Page removedPage : tablePagesEntry.getValue()) {
                    currentBytes -= removedPage.getRetainedSizeInBytes();
                }
                tablePages.remove();
                tableSizes.remove(tableId);
            }
        }
    }

    public static class SizeInfo
    {
        private final long rowCount;
        private final long byteSize;

        public SizeInfo(long rowCount, long byteSize)
        {
            this.rowCount = rowCount;
            this.byteSize = byteSize;
        }

        public long getRowCount()
        {
            return rowCount;
        }

        public long getsizeBytes()
        {
            return byteSize;
        }
    }
}
