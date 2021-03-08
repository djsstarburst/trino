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
package io.trino.plugin.testbed;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.testbed.TestbedErrorCode.MEMORY_LIMIT_EXCEEDED;
import static io.trino.plugin.testbed.TestbedErrorCode.MISSING_DATA;
import static java.lang.String.format;

@ThreadSafe
public class TestbedPagesStore
{
    private final long maxBytes;

    @GuardedBy("this")
    private long currentBytes;

    private final Map<Long, TableData> tables = new HashMap<>();

    @Inject
    public TestbedPagesStore(TestbedConfig config)
    {
        this.maxBytes = config.getMaxDataPerNode().toBytes();
    }

    public synchronized void initialize(long tableId)
    {
        if (!tables.containsKey(tableId)) {
            tables.put(tableId, new TableData());
        }
    }

    public synchronized void add(Long tableId, Page page)
    {
        if (!contains(tableId)) {
            throw new TrinoException(MISSING_DATA, "Failed to find table on a worker.");
        }

        page.compact();

        long newSize = currentBytes + page.getRetainedSizeInBytes();
        if (maxBytes < newSize) {
            throw new TrinoException(MEMORY_LIMIT_EXCEEDED, format("Testbed limit [%d] for memory connector exceeded", maxBytes));
        }
        currentBytes = newSize;

        TableData tableData = tables.get(tableId);
        tableData.add(page);
    }

    public synchronized List<Page> getPages(
            Long tableId,
            int partNumber,
            int totalParts,
            List<Integer> columnIndexes,
            long expectedRows,
            OptionalLong limit,
            OptionalDouble sampleRatio)
    {
        if (!contains(tableId)) {
            throw new TrinoException(MISSING_DATA, "Failed to find table on a worker.");
        }
        TableData tableData = tables.get(tableId);
        if (tableData.getRows() < expectedRows) {
            throw new TrinoException(MISSING_DATA,
                    format("Expected to find [%s] rows on a worker, but found [%s].", expectedRows, tableData.getRows()));
        }

        ImmutableList.Builder<Page> partitionedPages = ImmutableList.builder();

        boolean done = false;
        long totalRows = 0;
        for (int i = partNumber; i < tableData.getPages().size() && !done; i += totalParts) {
            if (sampleRatio.isPresent() && ThreadLocalRandom.current().nextDouble() >= sampleRatio.getAsDouble()) {
                continue;
            }

            Page page = tableData.getPages().get(i);
            totalRows += page.getPositionCount();
            if (limit.isPresent() && totalRows > limit.getAsLong()) {
                page = page.getRegion(0, (int) (page.getPositionCount() - (totalRows - limit.getAsLong())));
                done = true;
            }
            partitionedPages.add(getColumns(page, columnIndexes));
        }

        return partitionedPages.build();
    }

    public synchronized boolean contains(Long tableId)
    {
        return tables.containsKey(tableId);
    }

    public synchronized void cleanUp(Set<Long> activeTableIds)
    {
        // We have to remember that there might be some race conditions when there are two tables created at once.
        // That can lead to a situation when TestbedPagesStore already knows about a newer second table on some worker
        // but cleanUp is triggered by insert from older first table, which TestbedTableHandle was created before
        // second table creation. Thus activeTableIds can have missing latest ids and we can only clean up tables
        // that:
        // - have smaller value then max(activeTableIds).
        // - are missing from activeTableIds set

        if (activeTableIds.isEmpty()) {
            // if activeTableIds is empty, we cannot determine latestTableId...
            return;
        }
        long latestTableId = Collections.max(activeTableIds);

        for (Iterator<Map.Entry<Long, TableData>> tableDataIterator = tables.entrySet().iterator(); tableDataIterator.hasNext(); ) {
            Map.Entry<Long, TableData> tablePagesEntry = tableDataIterator.next();
            Long tableId = tablePagesEntry.getKey();
            if (tableId < latestTableId && !activeTableIds.contains(tableId)) {
                for (Page removedPage : tablePagesEntry.getValue().getPages()) {
                    currentBytes -= removedPage.getRetainedSizeInBytes();
                }
                tableDataIterator.remove();
            }
        }
    }

    private static Page getColumns(Page page, List<Integer> columnIndexes)
    {
        Block[] outputBlocks = new Block[columnIndexes.size()];

        for (int i = 0; i < columnIndexes.size(); i++) {
            outputBlocks[i] = page.getBlock(columnIndexes.get(i));
        }

        return new Page(page.getPositionCount(), outputBlocks);
    }

    private static final class PageRows
    {
        private final Page page;
        private final int pageNumber;

        public PageRows(Page page, int pageNumber)
        {
            this.page = page;
            this.pageNumber = pageNumber;
        }

        public Page getPage()
        {
            return page;
        }

        public int getPageNumber()
        {
            return pageNumber;
        }
    }

    private static final class TableData
    {
        private final AtomicInteger pageNumberCounter = new AtomicInteger();
        private final List<PageRows> pages = new ArrayList<>();
        private long rows;

        public void add(Page page)
        {
            int pageNumber = pageNumberCounter.incrementAndGet();
            // Add two columns for the page: pageNumber and rowId
            int channelCount = page.getChannelCount();
            Block[] pageBlocks = new Block[channelCount + 2];
            for (int i = 0; i < channelCount; i++) {
                pageBlocks[i] = page.getBlock(i);
            }
            int positionCount = page.getPositionCount();
            pageBlocks[channelCount] = new RunLengthEncodedBlock(new IntArrayBlock(1, Optional.empty(), new int[] {pageNumber}), positionCount);
            int[] rowNumbers = IntStream.range(0, positionCount).toArray();
            pageBlocks[channelCount + 1] = new IntArrayBlock(positionCount, Optional.empty(), rowNumbers);
            Page pageWithRowIdColumns = new Page(pageBlocks);
            pages.add(new PageRows(pageWithRowIdColumns, pageNumber));
            rows += page.getPositionCount();
        }

        private List<Page> getPages()
        {
            return pages.stream().map(PageRows::getPage).collect(toImmutableList());
        }

        private long getRows()
        {
            return rows;
        }
    }
}
