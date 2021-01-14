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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.sql.tree.ClassifierFunction;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.Label;
import io.trino.sql.tree.LabelDereference;
import io.trino.sql.tree.MatchNumberFunction;
import io.trino.sql.tree.PatternNavigationFunction;
import io.trino.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.sql.tree.ProcessingMode.Mode.FINAL;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Rewriter for expressions specific to row pattern recognition.
 * Removes label-prefixed symbol references from the expression and replaces them with symbols.
 * Removes row pattern navigation functions (PREV, NEXT, FIRST and LAST) from the expression.
 * Removes pattern special functions CLASSIFIER() and MATCH_NUMBER() and replaces them with symbols.
 * Reallocates all symbols in the expression to avoid unwanted optimizations when the expression is compiled.
 * For each of the symbols creates a value accessor (ValuePointer).
 * Returns new symbols as expected "input descriptor", upon which the rewritten expression will be compiled, along with value accessors.
 * Value accessors are ordered the same way as the corresponding symbols so that they can be used to provide actual values to the compiled expression.
 * Each time the compiled expression will be executed, a single-row input will be prepared with the use of the value accessors,
 * following the symbols layout.
 */
public class LogicalIndexExtractor
{
    public static ExpressionAndLayout rewrite(Expression expression, Map<Label, Set<Label>> subsets)
    {
        ImmutableList.Builder<Symbol> layout = ImmutableList.builder();
        ImmutableList.Builder<ValuePointer> valuePointers = ImmutableList.builder();
        ImmutableSet.Builder<Symbol> classifierSymbols = ImmutableSet.builder();
        ImmutableSet.Builder<Symbol> matchNumberSymbols = ImmutableSet.builder();

        Visitor visitor = new Visitor(subsets, layout, valuePointers, classifierSymbols, matchNumberSymbols);
        Expression rewritten = ExpressionTreeRewriter.rewriteWith(visitor, expression, LogicalIndexContext.DEFAULT);

        return new ExpressionAndLayout(rewritten, layout.build(), valuePointers.build(), classifierSymbols.build(), matchNumberSymbols.build());
    }

    private LogicalIndexExtractor() {}

    private static class Visitor
            extends ExpressionRewriter<LogicalIndexContext>
    {
        private final Map<Label, Set<Label>> subsets;
        private final ImmutableList.Builder<Symbol> layout;
        private final ImmutableList.Builder<ValuePointer> valuePointers;
        private final ImmutableSet.Builder<Symbol> classifierSymbols;
        private final ImmutableSet.Builder<Symbol> matchNumberSymbols;
        private int copyIndex;

        public Visitor(Map<Label, Set<Label>> subsets, ImmutableList.Builder<Symbol> layout, ImmutableList.Builder<ValuePointer> valuePointers, ImmutableSet.Builder<Symbol> classifierSymbols, ImmutableSet.Builder<Symbol> matchNumberSymbols)
        {
            this.subsets = requireNonNull(subsets, "subsets is null");
            this.layout = requireNonNull(layout, "layout is null");
            this.valuePointers = requireNonNull(valuePointers, "valuePointers is null");
            this.classifierSymbols = requireNonNull(classifierSymbols, "classifierSymbols is null");
            this.matchNumberSymbols = requireNonNull(matchNumberSymbols, "matchNumberSymbols is null");
            this.copyIndex = 0;
        }

        @Override
        protected Expression rewriteExpression(Expression node, LogicalIndexContext context, ExpressionTreeRewriter<LogicalIndexContext> treeRewriter)
        {
            return treeRewriter.defaultRewrite(node, context);
        }

        @Override
        public Expression rewritePatternNavigationFunction(PatternNavigationFunction node, LogicalIndexContext context, ExpressionTreeRewriter<LogicalIndexContext> treeRewriter)
        {
            switch (node.getType()) {
                case PREV:
                    return treeRewriter.rewrite(node.getArgument(), context.withPhysicalOffset(-toIntExact(node.getOffset())));
                case NEXT:
                    return treeRewriter.rewrite(node.getArgument(), context.withPhysicalOffset(toIntExact(node.getOffset())));
                case FIRST:
                    boolean running = node.getProcessingMode().isEmpty() || node.getProcessingMode().get() != FINAL;
                    return treeRewriter.rewrite(node.getArgument(), context.withLogicalOffset(running, false, toIntExact(node.getOffset())));
                case LAST:
                    running = node.getProcessingMode().isEmpty() || node.getProcessingMode().get() != FINAL;
                    return treeRewriter.rewrite(node.getArgument(), context.withLogicalOffset(running, true, toIntExact(node.getOffset())));
                default:
                    throw new IllegalStateException("unsupported pattern navigation function type: " + node.getType());
            }
        }

        @Override
        public Expression rewriteLabelDereference(LabelDereference node, LogicalIndexContext context, ExpressionTreeRewriter<LogicalIndexContext> treeRewriter)
        {
            Symbol reallocated = new Symbol(node.getReference().getName() + "_copy_" + copyIndex++);
            layout.add(reallocated);
            Set<Label> labels = subsets.get(node.getLabel());
            if (labels == null) {
                labels = ImmutableSet.of(node.getLabel());
            }
            valuePointers.add(new ValuePointer(context.withLabels(labels).toLogicalIndexPointer(), Symbol.from(node.getReference())));
            return reallocated.toSymbolReference();
        }

        @Override
        public Expression rewriteSymbolReference(SymbolReference node, LogicalIndexContext context, ExpressionTreeRewriter<LogicalIndexContext> treeRewriter)
        {
            // symbol reference with no label prefix is implicitly prefixed with a universal row pattern variable (matches every label)
            // it is encoded as empty label set
            Symbol reallocated = new Symbol(node.getName() + "_copy_" + copyIndex++);
            layout.add(reallocated);
            valuePointers.add(new ValuePointer(context.withLabels(ImmutableSet.of()).toLogicalIndexPointer(), Symbol.from(node)));
            return reallocated.toSymbolReference();
        }

        @Override
        public Expression rewriteClassifierFunction(ClassifierFunction node, LogicalIndexContext context, ExpressionTreeRewriter<LogicalIndexContext> treeRewriter)
        {
            Symbol classifierSymbol = new Symbol("classifier_copy_" + copyIndex++);
            layout.add(classifierSymbol);

            Set<Label> labels = ImmutableSet.of();
            if (node.getLabel().isPresent()) {
                labels = subsets.get(node.getLabel().get());
                if (labels == null) {
                    labels = ImmutableSet.of(node.getLabel().get());
                }
            }
            // pass the new symbol as input symbol. It will be used to identify classifier function.
            valuePointers.add(new ValuePointer(context.withLabels(labels).toLogicalIndexPointer(), classifierSymbol));
            classifierSymbols.add(classifierSymbol);
            return classifierSymbol.toSymbolReference();
        }

        @Override
        public Expression rewriteMatchNumberFunction(MatchNumberFunction node, LogicalIndexContext context, ExpressionTreeRewriter<LogicalIndexContext> treeRewriter)
        {
            Symbol matchNumberSymbol = new Symbol("match_number_copy_" + copyIndex++);
            layout.add(matchNumberSymbol);
            // pass default LogicalIndexPointer. It will not be accessed. match_number() is constant in the context of a match.
            // pass the new symbol as input symbol. It will be used to identify match number function.
            valuePointers.add(new ValuePointer(LogicalIndexContext.DEFAULT.toLogicalIndexPointer(), matchNumberSymbol));
            matchNumberSymbols.add(matchNumberSymbol);
            return matchNumberSymbol.toSymbolReference();
        }
    }

    private static class LogicalIndexContext
    {
        private final Set<Label> label;
        private final boolean running;
        private final boolean last;
        private final int logicalOffset;
        private final int physicalOffset;

        public static final LogicalIndexContext DEFAULT = new LogicalIndexContext(ImmutableSet.of(), true, true, 0, 0);

        private LogicalIndexContext(Set<Label> label, boolean running, boolean last, int logicalOffset, int physicalOffset)
        {
            this.label = requireNonNull(label, "label is null");
            this.running = running;
            this.last = last;
            this.logicalOffset = logicalOffset;
            this.physicalOffset = physicalOffset;
        }

        public LogicalIndexContext withPhysicalOffset(int physicalOffset)
        {
            return new LogicalIndexContext(this.label, this.running, this.last, this.logicalOffset, physicalOffset);
        }

        public LogicalIndexContext withLogicalOffset(boolean running, boolean last, int logicalOffset)
        {
            return new LogicalIndexContext(this.label, running, last, logicalOffset, this.physicalOffset);
        }

        public LogicalIndexContext withLabels(Set<Label> labels)
        {
            return new LogicalIndexContext(labels, this.running, this.last, this.logicalOffset, this.physicalOffset);
        }

        public LogicalIndexPointer toLogicalIndexPointer()
        {
            return new LogicalIndexPointer(label, last, running, logicalOffset, physicalOffset);
        }
    }

    public static class ExpressionAndLayout
    {
        private final Expression expression;
        private final List<Symbol> layout;
        private final List<ValuePointer> valuePointers;
        private final Set<Symbol> classifierSymbols;
        private final Set<Symbol> matchNumberSymbols;

        public ExpressionAndLayout(Expression expression, List<Symbol> layout, List<ValuePointer> valuePointers, Set<Symbol> classifierSymbols, Set<Symbol> matchNumberSymbols)
        {
            this.expression = requireNonNull(expression, "expression is null");
            this.layout = requireNonNull(layout, "layout is null");
            this.valuePointers = requireNonNull(valuePointers, "valuePointers is null");
            this.classifierSymbols = requireNonNull(classifierSymbols, "classifierSymbols is null");
            this.matchNumberSymbols = requireNonNull(matchNumberSymbols, "matchNumberSymbols is null");
        }

        public Expression getExpression()
        {
            return expression;
        }

        public List<Symbol> getLayout()
        {
            return layout;
        }

        public List<ValuePointer> getValuePointers()
        {
            return valuePointers;
        }

        public Set<Symbol> getClassifierSymbols()
        {
            return classifierSymbols;
        }

        public Set<Symbol> getMatchNumberSymbols()
        {
            return matchNumberSymbols;
        }
    }

    public static class ValuePointer
    {
        private final LogicalIndexPointer logicalIndexPointer;
        private final Symbol inputSymbol;

        public ValuePointer(LogicalIndexPointer logicalIndexPointer, Symbol inputSymbol)
        {
            this.logicalIndexPointer = requireNonNull(logicalIndexPointer, "logicalIndexPointer is null");
            this.inputSymbol = requireNonNull(inputSymbol, "inputSymbol is null");
        }

        public LogicalIndexPointer getLogicalIndexPointer()
        {
            return logicalIndexPointer;
        }

        public Symbol getInputSymbol()
        {
            return inputSymbol;
        }
    }
}
