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

public class RowPatternRewriter<C>
{
    protected RowPattern rewriteRowPattern(RowPattern node, C context, RowPatternTreeRewriter<C> treeRewriter)
    {
        return null;
    }

    public RowPattern rewriteAnchorPattern(AnchorPattern node, C context, RowPatternTreeRewriter<C> treeRewriter)
    {
        return rewriteRowPattern(node, context, treeRewriter);
    }

    public RowPattern rewriteEmptyPattern(EmptyPattern node, C context, RowPatternTreeRewriter<C> treeRewriter)
    {
        return rewriteRowPattern(node, context, treeRewriter);
    }

    public RowPattern rewriteExcludedPattern(ExcludedPattern node, C context, RowPatternTreeRewriter<C> treeRewriter)
    {
        return rewriteRowPattern(node, context, treeRewriter);
    }

    public RowPattern rewriteGroupedPattern(GroupedPattern node, C context, RowPatternTreeRewriter<C> treeRewriter)
    {
        return rewriteRowPattern(node, context, treeRewriter);
    }

    public RowPattern rewritePatternAlternation(PatternAlternation node, C context, RowPatternTreeRewriter<C> treeRewriter)
    {
        return rewriteRowPattern(node, context, treeRewriter);
    }

    public RowPattern rewritePatternConcatenation(PatternConcatenation node, C context, RowPatternTreeRewriter<C> treeRewriter)
    {
        return rewriteRowPattern(node, context, treeRewriter);
    }

    public RowPattern rewritePatternLabel(PatternLabel node, C context, RowPatternTreeRewriter<C> treeRewriter)
    {
        return rewriteRowPattern(node, context, treeRewriter);
    }

    public RowPattern rewritePatternPermutation(PatternPermutation node, C context, RowPatternTreeRewriter<C> treeRewriter)
    {
        return rewriteRowPattern(node, context, treeRewriter);
    }

    public RowPattern rewritePatternVariable(PatternVariable node, C context, RowPatternTreeRewriter<C> treeRewriter)
    {
        return rewriteRowPattern(node, context, treeRewriter);
    }

    public RowPattern rewriteQuantifiedPattern(QuantifiedPattern node, C context, RowPatternTreeRewriter<C> treeRewriter)
    {
        return rewriteRowPattern(node, context, treeRewriter);
    }
}
