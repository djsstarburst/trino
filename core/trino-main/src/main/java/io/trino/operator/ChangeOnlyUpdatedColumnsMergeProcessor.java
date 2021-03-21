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
package io.trino.operator;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.connector.RowChangeParadigm.CHANGE_ONLY_UPDATED_COLUMNS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ChangeOnlyUpdatedColumnsMergeProcessor
        implements MergeRowChangeProcessor
{
    private final int writeRedistributionColumnCount;
    private final int rowIdChannel;
    private final int mergeRowChannel;
    private final List<Integer> dataColumnChannels;

    public ChangeOnlyUpdatedColumnsMergeProcessor(
            List<Type> dataColumnTypes,
            Type rowIdType,
            int rowIdChannel,
            int mergeRowChannel,
            List<Integer> redistributionColumnChannels,
            List<Integer> dataColumnChannels)
    {
        requireNonNull(dataColumnTypes, "dataColumnTypes is null");
        this.writeRedistributionColumnCount = redistributionColumnChannels.size();
        requireNonNull(rowIdType, "rowIdType is null");
        this.rowIdChannel = rowIdChannel;
        this.mergeRowChannel = mergeRowChannel;
        this.dataColumnChannels = requireNonNull(dataColumnChannels, "dataColumnChannels is null");
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm()
    {
        return CHANGE_ONLY_UPDATED_COLUMNS;
    }

    /**
     * Transform the input page containing the target table's write redistribution column
     * blocks; the rowId block; and the merge case RowBlock. Each row in the output Page
     * starts with all the data column blocks, including the partition columns blocks,
     * table column order, followed by a boolean column indicating whether the source row
     * matched any target row, followed by the "operation" block from the merge case RowBlock,
     * whose values are {@link ConnectorMergeSink#INSERT_OPERATION_NUMBER},
     * {@link ConnectorMergeSink#DELETE_OPERATION_NUMBER}, or
     * {@link ConnectorMergeSink#UPDATE_OPERATION_NUMBER}
     * @param inputPage The page to be transformed.
     * @return A page containing all data column blocks, followed by the operation block.
     */
    @Override
    public Page transformPage(Page inputPage)
    {
        requireNonNull(inputPage, "inputPage is null");
        int inputChannelCount = inputPage.getChannelCount();
        if (inputChannelCount < 2 + writeRedistributionColumnCount) {
            throw new IllegalArgumentException(format("inputPage channelCount (%s) should be >= 2 + %s", inputChannelCount, writeRedistributionColumnCount));
        }

        int positionCount = inputPage.getPositionCount();
        if (positionCount <= 0) {
            throw new IllegalArgumentException("positionCount should be > 0, but is " + positionCount);
        }

        Block mergeCaseBlock = inputPage.getBlock(mergeRowChannel);

        List<Block> mergeCaseBlocks = mergeCaseBlock.getChildren();
        int mergeBlocksSize = mergeCaseBlocks.size();
        Block operationChannelBlock = mergeCaseBlocks.get(mergeBlocksSize - 2);

        List<Block> builder = new ArrayList<>();
        dataColumnChannels.forEach(channel -> builder.add(mergeCaseBlocks.get(channel)));
        builder.add(operationChannelBlock);

        // The rowId block is the last block of the resulting page
        Block rowIdBlock = inputPage.getBlock(rowIdChannel);
        builder.add(rowIdBlock);
        Page result = new Page(builder.toArray(new Block[]{}));

        int defaultCaseCount = 0;
        for (int position = 0; position < positionCount; position++) {
            if (TINYINT.getLong(operationChannelBlock, position) == DEFAULT_CASE_OPERATION_NUMBER) {
                defaultCaseCount++;
            }
        }
        if (defaultCaseCount == 0) {
            return result;
        }
        int usedCases = 0;
        int[] positions = new int[positionCount - defaultCaseCount];
        for (int position = 0; position < positionCount; position++) {
            if (TINYINT.getLong(operationChannelBlock, position) != DEFAULT_CASE_OPERATION_NUMBER) {
                positions[usedCases] = position;
                usedCases++;
            }
        }
        checkArgument(usedCases + defaultCaseCount == positionCount, "usedCases (%s) + defaultCaseCount (%s) != positionCount (%s)", usedCases, defaultCaseCount, positionCount);
        return result.getPositions(positions, 0, usedCases);
    }
}
