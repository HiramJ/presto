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
import com.facebook.presto.plugin.turbonium.remote.PageStore;
import com.facebook.presto.spi.Page;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.List;
import java.util.Set;

public class TurboniumPagesStore
{
    private static final Logger log = Logger.get(TurboniumPagesStore.class);
    private final TurboniumConfigManager configManager;
    private final PageStore pageStore;

    @Inject
    public TurboniumPagesStore(TurboniumConfigManager configManager, PageStore pageStore)
    {
        this.configManager = configManager;
        this.pageStore = pageStore;
    }

    public void initialize(String tableName, long tableId)
    {
        pageStore.initialize(tableName, tableId);
    }

    public void add(Long tableId, Page page)
    {
        pageStore.add(tableId, page);
    }

    public List<Page> getPages(Long tableId, int partNumber, int totalParts, List<Integer> columnIndexes)
    {
        return pageStore.getPages(tableId, partNumber, totalParts, columnIndexes);
    }

    public List<Long> listTableIds()
    {
        return pageStore.listTableIds();
    }

    public SizeInfo getSize(Long tableId)
    {
        PageStore.SizeInfo sizeInfo = pageStore.getSize(tableId);
        return new SizeInfo(sizeInfo.getRowCount(), sizeInfo.getsizeBytes());
    }

    public boolean contains(Long tableId)
    {
        return pageStore.contains(tableId);
    }

    public void cleanUp(Set<Long> activeTableIds)
    {
        pageStore.cleanUp(activeTableIds);
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
