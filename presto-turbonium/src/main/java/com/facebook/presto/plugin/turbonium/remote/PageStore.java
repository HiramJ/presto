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

import com.facebook.presto.spi.Page;

import java.util.List;
import java.util.Set;

public interface PageStore
{
    void initialize(String tableName, long tableId);

    void add(Long tableId, Page page);

    List<Page> getPages(Long tableId, int partNumber, int totalParts, List<Integer> columnIndexes);

    List<Long> listTableIds();

    SizeInfo getSize(Long tableId);

    boolean contains(Long tableId);

    void cleanUp(Set<Long> activeTableIds);

    class SizeInfo
    {
        private long rowCount;
        private long byteSize;

        public SizeInfo(long rowCount, long byteSize)
        {
            this.rowCount = rowCount;
            this.byteSize = byteSize;
        }

        public synchronized void update(long rowDelta, long byteSizeDelta)
        {
            this.rowCount += rowDelta;
            this.byteSize += byteSizeDelta;
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
