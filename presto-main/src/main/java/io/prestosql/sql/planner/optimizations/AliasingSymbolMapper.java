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
package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.sql.planner.OrderingScheme;
import io.prestosql.sql.planner.PartitioningScheme;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolAllocator;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.DistinctLimitNode;
import io.prestosql.sql.planner.plan.GroupIdNode;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.sql.planner.plan.RowNumberNode;
import io.prestosql.sql.planner.plan.StatisticAggregations;
import io.prestosql.sql.planner.plan.StatisticsWriterNode;
import io.prestosql.sql.planner.plan.TableFinishNode;
import io.prestosql.sql.planner.plan.TableWriterNode;
import io.prestosql.sql.planner.plan.TopNNode;
import io.prestosql.sql.planner.plan.TopNRowNumberNode;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionRewriter;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.SymbolReference;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.sql.planner.plan.AggregationNode.groupingSets;
import static java.util.Objects.requireNonNull;

/**
 * Maps symbols accordingly to the provided symbol map.
 * For certain symbols, specified as forbiddenSymbols, new mappings can be introduced and added to the symbol map.
 */
public class AliasingSymbolMapper
{
    private final SymbolAllocator symbolAllocator;
    private final Map<Symbol, Symbol> mapping;
    private final Set<Symbol> forbiddenSymbols;

    public AliasingSymbolMapper(SymbolAllocator symbolAllocator, Map<Symbol, Symbol> mapping, Set<Symbol> forbiddenSymbols)
    {
        this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        this.mapping = new HashMap<>(requireNonNull(mapping, "mapping is null"));
        this.forbiddenSymbols = ImmutableSet.copyOf(requireNonNull(forbiddenSymbols, "forbiddenSymbols is null"));
    }

    public AliasingSymbolMapper(Map<Symbol, Symbol> mapping)
    {
        this.symbolAllocator = null;
        this.mapping = ImmutableMap.copyOf(requireNonNull(mapping, "mapping is null"));
        this.forbiddenSymbols = ImmutableSet.of();
    }

    public Map<Symbol, Symbol> getMapping()
    {
        return ImmutableMap.copyOf(mapping);
    }

    // Return the canonical mapping for the symbol.
    // If the symbol is not allowed ("forbidden") and hasn't got a mapping yet,
    // map it to a new symbol. Following occurrences of the symbol will be mapped to the new symbol.
    public Symbol map(Symbol symbol)
    {
        while (mapping.containsKey(symbol)) {
            symbol = mapping.get(symbol);
        }

        if (forbiddenSymbols.contains(symbol)) {
            Symbol newSymbol = symbolAllocator.newSymbol(symbol);
            mapping.put(symbol, newSymbol);
            return newSymbol;
        }

        return symbol;
    }

    public List<Symbol> map(List<Symbol> symbols)
    {
        return symbols.stream()
                .map(this::map)
                .collect(toImmutableList());
    }

    public List<Symbol> mapAndDistinct(List<Symbol> symbols)
    {
        return symbols.stream()
                .map(this::map)
                .distinct()
                .collect(toImmutableList());
    }

    public Expression map(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteSymbolReference(SymbolReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Symbol canonical = map(Symbol.from(node));
                return canonical.toSymbolReference();
            }
        }, expression);
    }

    public AggregationNode map(AggregationNode node, PlanNode source)
    {
        ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> newAggregations = ImmutableMap.builder();
        for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
            newAggregations.put(map(entry.getKey()), map(entry.getValue()));
        }

        return new AggregationNode(
                node.getId(),
                source,
                newAggregations.build(),
                groupingSets(
                        mapAndDistinct(node.getGroupingKeys()),
                        node.getGroupingSetCount(),
                        node.getGlobalGroupingSets()),
                ImmutableList.of(),
                node.getStep(),
                node.getHashSymbol().map(this::map),
                node.getGroupIdSymbol().map(this::map));
    }

    private AggregationNode.Aggregation map(AggregationNode.Aggregation aggregation)
    {
        return new AggregationNode.Aggregation(
                aggregation.getResolvedFunction(),
                aggregation.getArguments().stream()
                        .map(this::map)
                        .collect(toImmutableList()),
                aggregation.isDistinct(),
                aggregation.getFilter().map(this::map),
                aggregation.getOrderingScheme().map(this::map),
                aggregation.getMask().map(this::map));
    }

    public GroupIdNode map(GroupIdNode node, PlanNode source) // TODO new
    {
        Map<Symbol, Symbol> newGroupingMappings = new HashMap<>();
        ImmutableList.Builder<List<Symbol>> newGroupingSets = ImmutableList.builder();

        for (List<Symbol> groupingSet : node.getGroupingSets()) {
            ImmutableList.Builder<Symbol> newGroupingSet = ImmutableList.builder();
            for (Symbol output : groupingSet) {
                Symbol newOutput = map(output);
                newGroupingMappings.putIfAbsent(
                        newOutput,
                        map(node.getGroupingColumns().get(output)));
                newGroupingSet.add(newOutput);
            }
            newGroupingSets.add(newGroupingSet.build());
        }

        return new GroupIdNode(
                node.getId(),
                source,
                newGroupingSets.build(),
                newGroupingMappings,
                mapAndDistinct(node.getAggregationArguments()),
                map(node.getGroupIdSymbol()));
    }

    public WindowNode map(WindowNode node, PlanNode source) // TODO new
    {
        ImmutableMap.Builder<Symbol, WindowNode.Function> newFunctions = ImmutableMap.builder();
        node.getWindowFunctions().forEach((symbol, function) -> {
            List<Expression> newArguments = function.getArguments().stream()
                    .map(this::map)
                    .collect(toImmutableList());
            WindowNode.Frame newFrame = map(function.getFrame());

            newFunctions.put(map(symbol), new WindowNode.Function(function.getResolvedFunction(), newArguments, newFrame, function.isIgnoreNulls()));
        });

        return new WindowNode(
                node.getId(),
                source,
                mapAndDistinct(node.getSpecification()),
                newFunctions.build(),
                node.getHashSymbol().map(this::map),
                node.getPrePartitionedInputs().stream()
                        .map(this::map)
                        .collect(toImmutableSet()),
                node.getPreSortedOrderPrefix());
    }

    private WindowNode.Frame map(WindowNode.Frame frame)
    {
        return new WindowNode.Frame(
                frame.getType(),
                frame.getStartType(),
                frame.getStartValue().map(this::map),
                frame.getEndType(),
                frame.getEndValue().map(this::map),
                frame.getOriginalStartValue(),
                frame.getOriginalEndValue());
    }

    private WindowNode.Specification mapAndDistinct(WindowNode.Specification specification)
    {
        return new WindowNode.Specification(
                mapAndDistinct(specification.getPartitionBy()),
                specification.getOrderingScheme().map(this::map));
    }

    public LimitNode map(LimitNode node, PlanNode source)
    {
        return new LimitNode(
                node.getId(),
                source,
                node.getCount(),
                node.getTiesResolvingScheme().map(this::map),
                node.isPartial());
    }

    public OrderingScheme map(OrderingScheme orderingScheme)
    {
        ImmutableList.Builder<Symbol> newSymbols = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, SortOrder> newOrderings = ImmutableMap.builder();
        Set<Symbol> added = new HashSet<>(orderingScheme.getOrderBy().size());
        for (Symbol symbol : orderingScheme.getOrderBy()) {
            Symbol canonical = map(symbol);
            if (added.add(canonical)) {
                newSymbols.add(canonical);
                newOrderings.put(canonical, orderingScheme.getOrdering(symbol));
            }
        }
        return new OrderingScheme(newSymbols.build(), newOrderings.build());
    }

    public DistinctLimitNode map(DistinctLimitNode node, PlanNode source) // TODO new
    {
        return new DistinctLimitNode(
                node.getId(),
                source,
                node.getLimit(),
                node.isPartial(),
                mapAndDistinct(node.getDistinctSymbols()),
                node.getHashSymbol().map(this::map));
    }

    public StatisticsWriterNode map(StatisticsWriterNode node, PlanNode source)
    {
        return new StatisticsWriterNode(
                node.getId(),
                source,
                node.getTarget(),
                map(node.getRowCountSymbol()),
                node.isRowCountEnabled(),
                node.getDescriptor().map(this::map));
    }

    public TableWriterNode map(TableWriterNode node, PlanNode source)
    {
        // Intentionally does not use mapAndDistinct on columns as that would remove columns
        return new TableWriterNode(
                node.getId(),
                source,
                node.getTarget(),
                map(node.getRowCountSymbol()),
                map(node.getFragmentSymbol()),
                map(node.getColumns()),
                node.getColumnNames(),
                node.getPartitioningScheme().map(partitioningScheme -> map(partitioningScheme, source.getOutputSymbols())),
                node.getStatisticsAggregation().map(this::map),
                node.getStatisticsAggregationDescriptor().map(descriptor -> descriptor.map(this::map)));
    }

    public PartitioningScheme map(PartitioningScheme scheme, List<Symbol> sourceLayout)
    {
        return new PartitioningScheme(
                scheme.getPartitioning().translate(this::map),
                mapAndDistinct(sourceLayout),
                scheme.getHashColumn().map(this::map),
                scheme.isReplicateNullsAndAny(),
                scheme.getBucketToPartition());
    }

    public TableFinishNode map(TableFinishNode node, PlanNode source)
    {
        return new TableFinishNode(
                node.getId(),
                source,
                node.getTarget(),
                map(node.getRowCountSymbol()),
                node.getStatisticsAggregation().map(this::map),
                node.getStatisticsAggregationDescriptor().map(descriptor -> descriptor.map(this::map)));
    }

    private StatisticAggregations map(StatisticAggregations statisticAggregations)
    {
        Map<Symbol, AggregationNode.Aggregation> aggregations = statisticAggregations.getAggregations().entrySet().stream()
                .collect(toImmutableMap(entry -> map(entry.getKey()), entry -> map(entry.getValue())));
        return new StatisticAggregations(aggregations, mapAndDistinct(statisticAggregations.getGroupingSymbols()));
    }

    public RowNumberNode map(RowNumberNode node, PlanNode source) //TODO new
    {
        return new RowNumberNode(
                node.getId(),
                source,
                mapAndDistinct(node.getPartitionBy()),
                node.isOrderSensitive(),
                map(node.getRowNumberSymbol()),
                node.getMaxRowCountPerPartition(),
                node.getHashSymbol().map(this::map));
    }

    public TopNRowNumberNode map(TopNRowNumberNode node, PlanNode source) //TODO new
    {
        return new TopNRowNumberNode(
                node.getId(),
                source,
                mapAndDistinct(node.getSpecification()),
                map(node.getRowNumberSymbol()),
                node.getMaxRowCountPerPartition(),
                node.isPartial(),
                node.getHashSymbol().map(this::map));
    }

    public TopNNode map(TopNNode node, PlanNode source)
    {
        return map(node, source, node.getId());
    }

    public TopNNode map(TopNNode node, PlanNode source, PlanNodeId nodeId)
    {
        return new TopNNode(
                nodeId,
                source,
                node.getCount(),
                map(node.getOrderingScheme()),
                node.getStep());
    }

    public static AliasingSymbolMapper.Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final ImmutableMap.Builder<Symbol, Symbol> mappings = ImmutableMap.builder();

        public void put(Symbol from, Symbol to)
        {
            mappings.put(from, to);
        }

        public AliasingSymbolMapper build()
        {
            return new AliasingSymbolMapper(mappings.build());
        }
    }
}
