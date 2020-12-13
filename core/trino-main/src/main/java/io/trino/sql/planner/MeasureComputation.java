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
package io.trino.sql.planner;

import io.trino.Session;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.Work;
import io.trino.operator.project.PageProjection;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.function.WindowIndex;
import io.trino.spi.type.Type;
import io.trino.sql.tree.Label;

import java.util.List;
import java.util.OptionalInt;
import java.util.function.Supplier;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.project.SelectedPositions.positionsRange;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.PhysicalValuePointer.CLASSIFIER;
import static io.trino.sql.planner.PhysicalValuePointer.MATCH_NUMBER;
import static java.util.Objects.requireNonNull;

public class MeasureComputation
{
    // compiled computation of expression
    private final Supplier<PageProjection> projectionSupplier;

    // value accessors ordered as expected by the compiled projection
    private final List<PhysicalValuePointer> expectedLayout;

    // result type
    private final Type type;

    public MeasureComputation(Supplier<PageProjection> projectionSupplier, List<PhysicalValuePointer> expectedLayout, Type type)
    {
        this.projectionSupplier = requireNonNull(projectionSupplier, "projectionSupplier is null");
        this.expectedLayout = requireNonNull(expectedLayout, "expectedLayout is null");
        this.type = requireNonNull(type, "type is null");
    }

    public Type getType()
    {
        return type;
    }

    public Block compute(int currentRow, int matchedRowsCount, Stack<Label> matchedLabels, int partitionStart, int partitionEnd, int patternStart, long matchNumber, WindowIndex windowIndex, Session session)
    {
        // get values at appropriate positions and prepare input for the projection as an array of single-value blocks
        Block[] blocks = new Block[expectedLayout.size()];
        for (int i = 0; i < expectedLayout.size(); i++) {
            PhysicalValuePointer physicalValuePointer = expectedLayout.get(i);
            int channel = physicalValuePointer.getSourceChannel();
            if (channel == MATCH_NUMBER) {
                blocks[i] = nativeValueToBlock(BIGINT, matchNumber);
            }
            else {
                OptionalInt position = physicalValuePointer.getLogicalIndexPointer().resolvePosition(currentRow, matchedRowsCount, matchedLabels, partitionStart, partitionEnd, patternStart);
                if (position.isPresent()) {
                    if (channel == CLASSIFIER) {
                        Type type = VARCHAR;
                        int resolvedPosition = position.getAsInt();
                        if (resolvedPosition < patternStart || resolvedPosition >= patternStart + matchedRowsCount) {
                            // position out of match. classifier() function returns null.
                            blocks[i] = nativeValueToBlock(type, null);
                        }
                        else {
                            // position within match. get the assigned label from matchedLabels.
                            // note: when computing measures, all labels of the match can be accessed (even those exceeding the current running position), both in RUNNING and FINAL semantics
                            blocks[i] = nativeValueToBlock(type, utf8Slice(matchedLabels.get(resolvedPosition - patternStart).getName()));
                        }
                    }
                    else {
                        // TODO Block#getRegion
                        blocks[i] = windowIndex.getSingleValueBlock(channel, position.getAsInt() - partitionStart);
                    }
                }
                else {
                    blocks[i] = nativeValueToBlock(physicalValuePointer.getType(), null);
                }
            }
        }

        // wrap block array into a single-row page
        Page page = new Page(1, blocks);

        // evaluate expression
        PageProjection projection = projectionSupplier.get();
        Work<Block> work = projection.project(session.toConnectorSession(), new DriverYieldSignal(), projection.getInputChannels().getInputChannels(page), positionsRange(0, 1));
        boolean done = false;
        while (!done) {
            done = work.process();
        }
        return work.getResult();
    }

    public Block computeEmpty(long matchNumber, Session session)
    {
        // prepare input for the projection as an array of single-value blocks. for empty match:
        // - match_number() is the sequential number of the match
        // - classifier() is null
        // - all value references are null
        Block[] blocks = new Block[expectedLayout.size()];
        for (int i = 0; i < expectedLayout.size(); i++) {
            PhysicalValuePointer physicalValuePointer = expectedLayout.get(i);
            if (physicalValuePointer.getSourceChannel() == MATCH_NUMBER) {
                blocks[i] = nativeValueToBlock(BIGINT, matchNumber);
            }
            else {
                blocks[i] = nativeValueToBlock(physicalValuePointer.getType(), null);
            }
        }

        // wrap block array into a single-row page
        Page page = new Page(1, blocks);

        // evaluate expression
        PageProjection projection = projectionSupplier.get();
        Work<Block> work = projection.project(session.toConnectorSession(), new DriverYieldSignal(), projection.getInputChannels().getInputChannels(page), positionsRange(0, 1));
        boolean done = false;
        while (!done) {
            done = work.process();
        }
        return work.getResult();
    }
}
