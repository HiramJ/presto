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
package com.facebook.presto.dht.filters;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.util.List;

public class ColumnBasedFilter
        implements PageFilter
{
    private static final int[] EMPTY = new int[0];
    @VisibleForTesting
    final int[] columns;

    public ColumnBasedFilter(byte[] colums)
    {
        this.columns = getColumns(colums);
    }

    public ColumnBasedFilter(List<Integer> columns)
    {
        this.columns = getColumns(columns);
    }

    private static int[] getColumns(List<Integer> columns)
    {
        if (columns == null || columns.size() == 0) {
            return EMPTY;
        }
        int[] ret = new int[columns.size()];
        for (int i = 0; i < ret.length; ++i) {
            ret[i] = columns.get(i);
        }

        return ret;
    }

    private static int[] getColumns(byte[] colums)
    {
        if (colums == null) {
            return EMPTY;
        }
        int size = colums.length / Integer.BYTES;
        int[] ret = new int[size];

        Slice slice = Slices.wrappedBuffer(colums);
        SliceInput input = slice.getInput();
        for (int i = 0; i < size; ++i) {
            ret[i] = input.readInt();
        }

        return ret;
    }

    @Override
    public Page filter(Page page)
    {
        Block[] blocks = page.getBlocks();
        Block[] outputBlocks = new Block[columns.length];

        for (int i = 0; i < columns.length; ++i) {
            outputBlocks[i] = blocks[columns[i]];
        }

        return new Page(page.getPositionCount(), outputBlocks);
    }

    @Override
    public byte[] toBytes()
    {
        Slice slice = Slices.allocate(Integer.BYTES * columns.length);
        SliceOutput output = slice.getOutput();
        for (int i : columns) {
            output.writeInt(i);
        }

        return slice.getBytes();
    }
}
