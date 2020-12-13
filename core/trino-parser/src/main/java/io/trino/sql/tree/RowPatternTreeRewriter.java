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
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Iterator;
import java.util.List;

public final class RowPatternTreeRewriter<C>
{
    private final RowPatternRewriter<C> rewriter;
    private final AstVisitor<RowPattern, RowPatternTreeRewriter.Context<C>> visitor;

    public static <T extends RowPattern> T rewriteWith(RowPatternRewriter<Void> rewriter, T node)
    {
        return new RowPatternTreeRewriter<>(rewriter).rewrite(node, null);
    }

    public static <C, T extends RowPattern> T rewriteWith(RowPatternRewriter<C> rewriter, T node, C context)
    {
        return new RowPatternTreeRewriter<>(rewriter).rewrite(node, context);
    }

    public RowPatternTreeRewriter(RowPatternRewriter<C> rewriter)
    {
        this.rewriter = rewriter;
        this.visitor = new RewritingVisitor();
    }

    private List<RowPattern> rewrite(List<RowPattern> items, Context<C> context)
    {
        ImmutableList.Builder<RowPattern> builder = ImmutableList.builder();
        for (RowPattern rowPattern : items) {
            builder.add(rewrite(rowPattern, context.get()));
        }
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    public <T extends RowPattern> T rewrite(T node, C context)
    {
        return (T) visitor.process(node, new Context<>(context, false));
    }

    /**
     * Invoke the default rewrite logic explicitly. Specifically, it skips the invocation of the row pattern rewriter for the provided node.
     */
    @SuppressWarnings("unchecked")
    public <T extends RowPattern> T defaultRewrite(T node, C context)
    {
        return (T) visitor.process(node, new Context<>(context, true));
    }

    private class RewritingVisitor
            extends AstVisitor<RowPattern, RowPatternTreeRewriter.Context<C>>
    {
        @Override
        protected RowPattern visitRowPattern(RowPattern node, Context<C> context)
        {
            // RewritingVisitor must have explicit support for each row pattern type, with a dedicated visit method,
            // so visitRowPattern() should never be called.
            throw new UnsupportedOperationException("visit() not implemented for " + node.getClass().getName());
        }

        @Override
        protected RowPattern visitAnchorPattern(AnchorPattern node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                RowPattern result = rewriter.rewriteAnchorPattern(node, context.get(), RowPatternTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        protected RowPattern visitEmptyPattern(EmptyPattern node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                RowPattern result = rewriter.rewriteEmptyPattern(node, context.get(), RowPatternTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        protected RowPattern visitExcludedPattern(ExcludedPattern node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                RowPattern result = rewriter.rewriteExcludedPattern(node, context.get(), RowPatternTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            RowPattern pattern = rewrite(node.getPattern(), context.get());

            if (pattern != node.getPattern()) {
                return new ExcludedPattern(pattern);
            }

            return node;
        }

        @Override
        protected RowPattern visitGroupedPattern(GroupedPattern node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                RowPattern result = rewriter.rewriteGroupedPattern(node, context.get(), RowPatternTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            RowPattern pattern = rewrite(node.getPattern(), context.get());

            if (pattern != node.getPattern()) {
                return new GroupedPattern(pattern);
            }

            return node;
        }

        @Override
        protected RowPattern visitPatternAlternation(PatternAlternation node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                RowPattern result = rewriter.rewritePatternAlternation(node, context.get(), RowPatternTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            List<RowPattern> patterns = rewrite(node.getPatterns(), context);

            if (!sameElements(node.getPatterns(), patterns)) {
                return new PatternAlternation(patterns);
            }

            return node;
        }

        @Override
        protected RowPattern visitPatternConcatenation(PatternConcatenation node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                RowPattern result = rewriter.rewritePatternConcatenation(node, context.get(), RowPatternTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            List<RowPattern> patterns = rewrite(node.getPatterns(), context);

            if (!sameElements(node.getPatterns(), patterns)) {
                return new PatternConcatenation(patterns);
            }

            return node;
        }

        @Override
        protected RowPattern visitPatternLabel(PatternLabel node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                RowPattern result = rewriter.rewritePatternLabel(node, context.get(), RowPatternTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        protected RowPattern visitPatternPermutation(PatternPermutation node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                RowPattern result = rewriter.rewritePatternPermutation(node, context.get(), RowPatternTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            List<RowPattern> patterns = rewrite(node.getPatterns(), context);

            if (!sameElements(node.getPatterns(), patterns)) {
                return new PatternPermutation(patterns);
            }

            return node;
        }

        @Override
        protected RowPattern visitPatternVariable(PatternVariable node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                RowPattern result = rewriter.rewritePatternVariable(node, context.get(), RowPatternTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        protected RowPattern visitQuantifiedPattern(QuantifiedPattern node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                RowPattern result = rewriter.rewriteQuantifiedPattern(node, context.get(), RowPatternTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            RowPattern pattern = rewrite(node.getPattern(), context.get());

            if (pattern != node.getPattern()) {
                return new QuantifiedPattern(pattern, node.getPatternQuantifier());
            }

            return node;
        }
    }

    public static class Context<C>
    {
        private final boolean defaultRewrite;
        private final C context;

        private Context(C context, boolean defaultRewrite)
        {
            this.context = context;
            this.defaultRewrite = defaultRewrite;
        }

        public C get()
        {
            return context;
        }

        public boolean isDefaultRewrite()
        {
            return defaultRewrite;
        }
    }

    @SuppressWarnings("ObjectEquality")
    private static <T> boolean sameElements(Iterable<? extends T> a, Iterable<? extends T> b)
    {
        if (Iterables.size(a) != Iterables.size(b)) {
            return false;
        }

        Iterator<? extends T> first = a.iterator();
        Iterator<? extends T> second = b.iterator();

        while (first.hasNext() && second.hasNext()) {
            if (first.next() != second.next()) {
                return false;
            }
        }

        return true;
    }
}
