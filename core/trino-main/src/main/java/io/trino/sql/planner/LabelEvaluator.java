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

import com.google.common.collect.ImmutableMap;
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
import java.util.Map;
import java.util.OptionalInt;
import java.util.function.Supplier;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.project.SelectedPositions.positionsRange;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.PhysicalValuePointer.CLASSIFIER;
import static io.trino.sql.planner.PhysicalValuePointer.MATCH_NUMBER;
import static io.trino.sql.tree.Label.EMPTY;
import static io.trino.sql.tree.Label.PARTITION_END;
import static io.trino.sql.tree.Label.PARTITION_START;
import static java.util.Objects.requireNonNull;

public class LabelEvaluator
{
    private final long matchNumber;

    private final int patternStart;

    // inclusive - the first row of the search partition
    private final int partitionStart;

    // inclusive - the last row of the search partition
    private final int partitionEnd;

    private final Map<Label, Evaluation> evaluations;

    private final WindowIndex windowIndex;

    private final Session session;

    public LabelEvaluator(long matchNumber, int patternStart, int partitionStart, int partitionEnd, Map<Label, Evaluation> evaluations, WindowIndex windowIndex, Session session)
    {
        this.matchNumber = matchNumber;
        this.patternStart = patternStart;
        this.partitionStart = partitionStart;
        this.partitionEnd = partitionEnd;
        this.evaluations = ImmutableMap.copyOf(requireNonNull(evaluations, "evaluations is null"));
        this.windowIndex = requireNonNull(windowIndex, "windowIndex is null");
        this.session = requireNonNull(session, "session is null");
    }

    public boolean evaluateLabel(Label label, int matchedRowsCount, Stack<Label> matchedLabels)
    {
        if (label.equals(EMPTY)) {
            return true;
        }

        if (label.equals(PARTITION_START)) {
            return patternStart == partitionStart && matchedRowsCount == 0;
        }

        if (label.equals(PARTITION_END)) {
            return patternStart + matchedRowsCount == partitionEnd;
        }

        if (patternStart + matchedRowsCount >= partitionEnd) {
            // reached partition end
            return false;
        }

        Evaluation evaluation = evaluations.get(label);

        if (evaluation == null) {
            // if there was no definition for this label in DEFINE clause, it always matches
            return true;
        }

        return evaluation.test(label, matchedRowsCount, matchedLabels, partitionStart, partitionEnd, patternStart, matchNumber, windowIndex, session);
    }

    public static class Evaluation
    {
        // compiled computation of label-defining boolean expression
        private final Supplier<PageProjection> projectionSupplier;

        // value accessors ordered as expected by the compiled projection
        private final List<PhysicalValuePointer> expectedLayout;

        public Evaluation(Supplier<PageProjection> projectionSupplier, List<PhysicalValuePointer> expectedLayout)
        {
            this.projectionSupplier = requireNonNull(projectionSupplier, "projectionSupplier is null");
            this.expectedLayout = requireNonNull(expectedLayout, "expectedLayout is null");
        }

        public boolean test(Label label, int matchedRowsCount, Stack<Label> matchedLabels, int partitionStart, int partitionEnd, int patternStart, long matchNumber, WindowIndex windowIndex, Session session)
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
                    OptionalInt position = physicalValuePointer.getLogicalIndexPointer().resolvePosition(label, matchedRowsCount, matchedLabels, partitionStart, partitionEnd, patternStart);
                    if (position.isPresent()) {
                        if (channel == CLASSIFIER) {
                            Type type = VARCHAR;
                            int resolvedPosition = position.getAsInt();
                            if (resolvedPosition < patternStart || resolvedPosition > patternStart + matchedRowsCount) {
                                // position out of match. classifier() function returns null.
                                blocks[i] = nativeValueToBlock(type, null);
                            }
                            else if (resolvedPosition == patternStart + matchedRowsCount) {
                                // currently matched position is considered to have the label temporarily assigned for the purpose of match evaluation. the label is not yet present in matchedLabels.
                                blocks[i] = nativeValueToBlock(type, utf8Slice(label.getName()));
                            }
                            else {
                                // position already matched. get the assigned label from matchedLabels.
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
            Block result = work.getResult();
            return BOOLEAN.getBoolean(result, 0);
        }
    }
}
