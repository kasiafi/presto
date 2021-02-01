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
package io.trino.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.trino.spi.type.Type;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.WindowNode.Frame;
import io.trino.sql.planner.plan.WindowNode.Specification;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Label;
import io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch;
import io.trino.sql.tree.RowPattern;
import io.trino.sql.tree.SkipTo.Position;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class PatternRecognitionNode
        extends PlanNode
{
    private final PlanNode source;
    private final Specification specification;
    private final Optional<Symbol> hashSymbol;
    private final Set<Symbol> prePartitionedInputs;
    private final int preSortedOrderPrefix;
    private final Map<Symbol, Measure> measures;
    private final Optional<Frame> commonBaseFrame;
    private final RowsPerMatch rowsPerMatch;
    private final Optional<Label> skipToLabel;
    private final Position skipToPosition;
    private final boolean initial;
    private final RowPattern pattern;
    private final Map<Label, Set<Label>> subsets;
    private final Map<Label, Expression> variableDefinitions;

    // TODO for pattern matching in window, we need to add regular window functions to the node.
    //    We want to have one pattern matching per one PatternRecognitionNode. For that, it is required that all window functions present in the node
    //    have the same frame, because the frame is a base for pattern matching. Operator takes the following steps:
    //      - determine common base frame
    //      - match the pattern in that frame
    //      - compute all measures on the match
    //      - compute all window functions in the reduced frame defined by the match
    //   Because the base frame is common to all window functions (and measures), it is a top-level property of PatternRecognitionNode, and not a property
    //   of particular window functions like in WindowNode. PatternRecognitionNodes can be merged as an optimization on the following conditions:
    //      - they have the same specifications
    //      - they have the same pattern recognition specification
    //      - they have the same common base frame

    @JsonCreator
    public PatternRecognitionNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("specification") Specification specification,
            @JsonProperty("hashSymbol") Optional<Symbol> hashSymbol,
            @JsonProperty("prePartitionedInputs") Set<Symbol> prePartitionedInputs,
            @JsonProperty("preSortedOrderPrefix") int preSortedOrderPrefix,
            @JsonProperty("measures") Map<Symbol, Measure> measures,
            @JsonProperty("commonBaseFrame") Optional<Frame> commonBaseFrame,
            @JsonProperty("rowsPerMatch") RowsPerMatch rowsPerMatch,
            @JsonProperty("skipToLabel") Optional<Label> skipToLabel,
            @JsonProperty("skipToPosition") Position skipToPosition,
            @JsonProperty("initial") boolean initial,
            @JsonProperty("pattern") RowPattern pattern,
            @JsonProperty("subsets") Map<Label, Set<Label>> subsets,
            @JsonProperty("variableDefinitions") Map<Label, Expression> variableDefinitions)

    {
        super(id);

        requireNonNull(source, "source is null");
        requireNonNull(specification, "specification is null");
        requireNonNull(hashSymbol, "hashSymbol is null");
        checkArgument(specification.getPartitionBy().containsAll(prePartitionedInputs), "prePartitionedInputs must be contained in partitionBy");
        Optional<OrderingScheme> orderingScheme = specification.getOrderingScheme();
        checkArgument(preSortedOrderPrefix == 0 || (orderingScheme.isPresent() && preSortedOrderPrefix <= orderingScheme.get().getOrderBy().size()), "Cannot have sorted more symbols than those requested");
        checkArgument(preSortedOrderPrefix == 0 || ImmutableSet.copyOf(prePartitionedInputs).equals(ImmutableSet.copyOf(specification.getPartitionBy())), "preSortedOrderPrefix can only be greater than zero if all partition symbols are pre-partitioned");
        requireNonNull(measures, "measures is null");
        requireNonNull(commonBaseFrame, "commonBaseFrame is null");
        requireNonNull(rowsPerMatch, "rowsPerMatch is null");
        requireNonNull(skipToLabel, "skipToLabel is null");
        requireNonNull(skipToPosition, "skipToPosition is null");
        requireNonNull(pattern, "pattern is null");
        requireNonNull(subsets, "subsets is null");
        requireNonNull(variableDefinitions, "variableDefinitions is null");

        this.source = source;
        this.specification = specification;
        this.hashSymbol = hashSymbol;
        this.prePartitionedInputs = ImmutableSet.copyOf(prePartitionedInputs);
        this.preSortedOrderPrefix = preSortedOrderPrefix;
        this.measures = ImmutableMap.copyOf(measures);
        this.commonBaseFrame = commonBaseFrame;
        this.rowsPerMatch = rowsPerMatch;
        this.skipToLabel = skipToLabel;
        this.skipToPosition = skipToPosition;
        this.initial = initial;
        this.pattern = pattern;
        this.subsets = subsets;
        this.variableDefinitions = ImmutableMap.copyOf(variableDefinitions);
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    // The order of symbols in the returned list might be different than expected layout of the node
    public List<Symbol> getOutputSymbols()
    {
        ImmutableList.Builder<Symbol> outputSymbols = ImmutableList.builder();
        if (rowsPerMatch.isOneRow()) {
            outputSymbols.addAll(getPartitionBy());
        }
        else {
            outputSymbols.addAll(source.getOutputSymbols());
        }
        outputSymbols.addAll(measures.keySet());

        return outputSymbols.build();
    }

    public Set<Symbol> getCreatedSymbols()
    {
        return ImmutableSet.copyOf(measures.keySet());
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public Specification getSpecification()
    {
        return specification;
    }

    public List<Symbol> getPartitionBy()
    {
        return specification.getPartitionBy();
    }

    public Optional<OrderingScheme> getOrderingScheme()
    {
        return specification.getOrderingScheme();
    }

    @JsonProperty
    public Optional<Symbol> getHashSymbol()
    {
        return hashSymbol;
    }

    @JsonProperty
    public Set<Symbol> getPrePartitionedInputs()
    {
        return prePartitionedInputs;
    }

    @JsonProperty
    public int getPreSortedOrderPrefix()
    {
        return preSortedOrderPrefix;
    }

    @JsonProperty
    public Map<Symbol, Measure> getMeasures()
    {
        return measures;
    }

    @JsonProperty
    public Optional<Frame> getCommonBaseFrame()
    {
        return commonBaseFrame;
    }

    @JsonProperty
    public RowsPerMatch getRowsPerMatch()
    {
        return rowsPerMatch;
    }

    @JsonProperty
    public Optional<Label> getSkipToLabel()
    {
        return skipToLabel;
    }

    @JsonProperty
    public Position getSkipToPosition()
    {
        return skipToPosition;
    }

    @JsonProperty
    public boolean isInitial()
    {
        return initial;
    }

    @JsonProperty
    public RowPattern getPattern()
    {
        return pattern;
    }

    @JsonProperty
    public Map<Label, Set<Label>> getSubsets()
    {
        return subsets;
    }

    @JsonProperty
    public Map<Label, Expression> getVariableDefinitions()
    {
        return variableDefinitions;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitPatternRecognition(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new PatternRecognitionNode(
                getId(),
                Iterables.getOnlyElement(newChildren),
                specification,
                hashSymbol,
                prePartitionedInputs,
                preSortedOrderPrefix,
                measures,
                commonBaseFrame,
                rowsPerMatch,
                skipToLabel,
                skipToPosition,
                initial,
                pattern,
                subsets,
                variableDefinitions);
    }

    public static class Measure
    {
        Expression expression;
        Type type;

        @JsonCreator
        public Measure(Expression expression, Type type)
        {
            this.expression = requireNonNull(expression, "expression is null");
            this.type = requireNonNull(type, "type is null");
        }

        @JsonProperty
        public Expression getExpression()
        {
            return expression;
        }

        @JsonProperty
        public Type getType()
        {
            return type;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Measure that = (Measure) o;
            return Objects.equals(expression, that.expression) &&
                    Objects.equals(type, that.type);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(expression, type);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("expression", expression)
                    .add("type", type)
                    .toString();
        }
    }
}