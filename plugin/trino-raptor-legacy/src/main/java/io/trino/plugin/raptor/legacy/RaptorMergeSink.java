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
package io.trino.plugin.raptor.legacy;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.raptor.legacy.storage.ShardRewriter;
import io.trino.plugin.raptor.legacy.storage.StorageManager;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.MergePage;
import io.trino.spi.type.UuidType;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.block.ColumnarRow.toColumnarRow;
import static io.trino.spi.connector.MergePage.createDeleteAndInsertPages;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.stream.Collectors.toUnmodifiableList;

public class RaptorMergeSink
        implements ConnectorMergeSink
{
    private static final Logger log = Logger.get(RaptorMergeSink.class);

    private final StorageManager storageManager;
    private final long transactionId;
    private final int columnCount;
    private final Map<UUID, BitSet> rowsToDelete = new HashMap<>();

    public RaptorMergeSink(StorageManager storageManager, long transactionId, int columnCount)
    {
        this.storageManager = requireNonNull(storageManager, "storageManager is null");
        this.transactionId = transactionId;
        this.columnCount = columnCount;
    }

    @Override
    public void storeMergedRows(Page page)
    {
        MergePage mergePage = createDeleteAndInsertPages(page, columnCount);

        mergePage.getDeletionsPage().ifPresent(deletions -> {
            ColumnarRow rowIdRow = toColumnarRow(deletions.getBlock(deletions.getChannelCount() - 1));
            Block shardUuidBlock = rowIdRow.getField(0);
            Block shardRowIdBlock = rowIdRow.getField(1);

            for (int position = 0; position < rowIdRow.getPositionCount(); position++) {
                UUID uuid = trinoUuidToJavaUuid(UuidType.UUID.getSlice(shardUuidBlock, position));
                int rowId = toIntExact(BIGINT.getLong(shardRowIdBlock, position));
                rowsToDelete.computeIfAbsent(uuid, ignored -> new BitSet()).set(rowId);
            }
        });

        mergePage.getInsertionsPage().ifPresent(insertions -> {
            // TODO
        });
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        List<CompletableFuture<Collection<Slice>>> futures = new ArrayList<>();

        rowsToDelete.forEach((uuid, rowIds) -> {
            log.info("Delete %s rows from shard %s", rowIds.cardinality(), uuid);
            OptionalInt bucketNumber = OptionalInt.empty();
            ShardRewriter rewriter = storageManager.createShardRewriter(transactionId, bucketNumber, uuid);
            futures.add(rewriter.rewrite(rowIds));
        });

        return allOf(futures.toArray(CompletableFuture[]::new))
                .thenApply(ignored -> futures.stream()
                        .map(CompletableFuture::join)
                        .flatMap(Collection::stream)
                        .collect(toUnmodifiableList()));
    }
}
