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
package com.facebook.presto.dht.common;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

public class PageUtil
{
    private PageUtil()
    {
    }

    public static void writeRawPage(Page page, SliceOutput output, BlockEncodingSerde serde)
    {
        output.writeInt(page.getPositionCount());
        Block[] blocks = page.getBlocks();
        output.writeInt(blocks.length);
        for (Block block : blocks) {
            writeBlock(serde, output, block);
        }
    }

    public static Page readRawPage(SliceInput input, BlockEncodingSerde blockEncodingSerde)
    {
        int positionCount = input.readInt();
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

    public static byte[] getPageBytes(Page page, BlockEncodingSerde blockEncodingSerde)
    {
        SliceOutput output = new DynamicSliceOutput((int) page.getSizeInBytes());
        PageUtil.writeRawPage(page, output, blockEncodingSerde);

        Slice slice = output.getUnderlyingSlice();
        return slice.getBytes();
    }
}
