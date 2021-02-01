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
import io.trino.sql.tree.AnchorPattern;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.EmptyPattern;
import io.trino.sql.tree.ExcludedPattern;
import io.trino.sql.tree.GroupedPattern;
import io.trino.sql.tree.Label;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.OneOrMoreQuantifier;
import io.trino.sql.tree.PatternAlternation;
import io.trino.sql.tree.PatternConcatenation;
import io.trino.sql.tree.PatternLabel;
import io.trino.sql.tree.PatternPermutation;
import io.trino.sql.tree.PatternVariable;
import io.trino.sql.tree.QuantifiedPattern;
import io.trino.sql.tree.RangeQuantifier;
import io.trino.sql.tree.RowPattern;
import io.trino.sql.tree.ZeroOrMoreQuantifier;
import io.trino.sql.tree.ZeroOrOneQuantifier;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Collections2.orderedPermutations;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.tree.Label.EMPTY;
import static java.lang.Math.max;

final class RowPatternNFABuilder
        extends AstVisitor<RowPatternNFA, Boolean>
{
    public static RowPatternNFA buildNFA(RowPattern node)
    {
        return new RowPatternNFABuilder().process(node, false);
    }

    @Override
    protected RowPatternNFA visitNode(Node node, Boolean inExclusion)
    {
        throw new IllegalStateException("unexpected node type: " + node.getClass().getName());
    }

    @Override
    protected RowPatternNFA visitRowPattern(RowPattern node, Boolean inExclusion)
    {
        throw new IllegalStateException("unsupported node type: " + node.getClass().getName());
    }

    @Override
    protected RowPatternNFA visitPatternVariable(PatternVariable node, Boolean inExclusion)
    {
        throw new IllegalStateException("unsupported node type: %s. Should be rewritten to PatternLabel" + node.getClass().getName());
    }

    @Override
    protected RowPatternNFA visitPatternLabel(PatternLabel node, Boolean inExclusion)
    {
        State end = new State();
        State start = new State(singleTransition(new Transition(node.getLabel(), end)));
        return new RowPatternNFA(start, end);
    }

    @Override
    protected RowPatternNFA visitAnchorPattern(AnchorPattern node, Boolean inExclusion)
    {
        State end = new State();
        Label label;
        switch (node.getType()) {
            case PARTITION_START:
                label = Label.PARTITION_START;
                break;
            case PARTITION_END:
                label = Label.PARTITION_END;
                break;
            default:
                throw new IllegalStateException("unexpected anchor type: " + node.getType());
        }
        State start = new State(singleTransition(new Transition(label, end)));
        return new RowPatternNFA(start, end);
    }

    @Override
    protected RowPatternNFA visitEmptyPattern(EmptyPattern node, Boolean inExclusion)
    {
        State end = new State();
        State start = new State(new LinkedList<>(ImmutableList.of(new Transition(EMPTY, end))));
        return new RowPatternNFA(start, end);
    }

    @Override
    protected RowPatternNFA visitGroupedPattern(GroupedPattern node, Boolean inExclusion)
    {
        // skip parentheses
        return process(node.getPattern(), inExclusion);
    }

    @Override
    protected RowPatternNFA visitExcludedPattern(ExcludedPattern node, Boolean inExclusion)
    {
        if (inExclusion) {
            // skip nested exclusion. this is necessary to resolve exclusions correctly during pattern matching
            return process(node.getPattern(), true);
        }

        RowPatternNFA child = process(node.getPattern(), true);

        State exclusionStart = new State(singleEmptyTransition(child.getStartState()), true, false);
        State exclusionEnd = new State(new LinkedList<>(), false, true);
        child.getEndState().withTransitions(singleEmptyTransition(exclusionEnd));

        State start = new State(singleEmptyTransition(exclusionStart));
        State end = new State(new LinkedList<>());
        exclusionEnd.withTransitions(singleEmptyTransition(end));

        return new RowPatternNFA(start, end);
    }

    @Override
    protected RowPatternNFA visitPatternAlternation(PatternAlternation node, Boolean inExclusion)
    {
        List<RowPatternNFA> children = node.getPatterns().stream()
                .map(pattern -> process(pattern, inExclusion))
                .collect(toImmutableList());
        State end = new State();
        LinkedList<Transition> startTransitions = new LinkedList<>();
        for (RowPatternNFA child : children) {
            startTransitions.add(new Transition(EMPTY, child.getStartState()));
            child.getEndState().withTransitions(singleEmptyTransition(end));
        }
        State start = new State(startTransitions);
        return new RowPatternNFA(start, end);
    }

    @Override
    protected RowPatternNFA visitPatternConcatenation(PatternConcatenation node, Boolean inExclusion)
    {
        List<RowPatternNFA> children = node.getPatterns().stream()
                .map(pattern -> process(pattern, inExclusion))
                .collect(toImmutableList());
        State start = new State(singleEmptyTransition(children.get(0).getStartState()));
        for (int i = 0; i < children.size() - 1; i++) {
            children.get(i).getEndState().withTransitions(singleEmptyTransition(children.get(i + 1).getStartState()));
        }
        State end = new State();
        children.get(children.size() - 1).getEndState().withTransitions(singleEmptyTransition(end));
        return new RowPatternNFA(start, end);
    }

    @Override
    protected RowPatternNFA visitPatternPermutation(PatternPermutation node, Boolean inExclusion)
    {
        Iterator<List<Integer>> permutations = orderedPermutations(IntStream.range(0, node.getPatterns().size())
                .boxed()
                .collect(toImmutableList()))
                .iterator();
        ImmutableList.Builder<RowPatternNFA> alternatives = ImmutableList.builder();

        while (permutations.hasNext()) {
            // process parts (new copy needed for every permutation)
            List<RowPatternNFA> parts = node.getPatterns().stream()
                    .map(pattern -> process(pattern, inExclusion))
                    .collect(toImmutableList());
            // order parts accordingly to the current permutation
            List<Integer> permutation = permutations.next();
            ImmutableList.Builder<RowPatternNFA> builder = ImmutableList.builder();
            for (int index : permutation) {
                builder.add(parts.get(index));
            }
            List<RowPatternNFA> permutedParts = builder.build();
            // concatenate parts
            for (int i = 0; i < permutedParts.size() - 1; i++) {
                permutedParts.get(i).getEndState().withTransitions(singleEmptyTransition(permutedParts.get(i + 1).getStartState()));
            }
            alternatives.add(new RowPatternNFA(permutedParts.get(0).getStartState(), permutedParts.get(permutedParts.size() - 1).getEndState()));
        }

        // build alternation of all permutations
        State end = new State();
        LinkedList<Transition> startTransitions = new LinkedList<>();
        for (RowPatternNFA permutation : alternatives.build()) {
            startTransitions.add(new Transition(EMPTY, permutation.getStartState()));
            permutation.getEndState().withTransitions(singleEmptyTransition(end));
        }
        State start = new State(startTransitions);

        return new RowPatternNFA(start, end);
    }

    @Override
    protected RowPatternNFA visitQuantifiedPattern(QuantifiedPattern node, Boolean inExclusion)
    {
        if (node.getPatternQuantifier() instanceof ZeroOrMoreQuantifier) {
            return loopingQuantified(node, 0, inExclusion);
        }
        if (node.getPatternQuantifier() instanceof OneOrMoreQuantifier) {
            return loopingQuantified(node, 1, inExclusion);
        }
        if (node.getPatternQuantifier() instanceof ZeroOrOneQuantifier) {
            return rangeQuantified(node, 0, 1, inExclusion);
        }
        if (node.getPatternQuantifier() instanceof RangeQuantifier) {
            RangeQuantifier quantifier = (RangeQuantifier) node.getPatternQuantifier();
            int min = quantifier.getAtLeast().orElse(0);
            if (quantifier.getAtMost().isPresent()) {
                return rangeQuantified(node, min, quantifier.getAtMost().get(), inExclusion);
            }
            return loopingQuantified(node, min, inExclusion);
        }
        throw new IllegalStateException("unexpected PatternQuantifier type: " + node.getPatternQuantifier().getClass().getSimpleName());
    }

    private RowPatternNFA loopingQuantified(QuantifiedPattern node, int min, boolean inExclusion)
    {
        // chain min copies of the subpattern or 1 copy of the subpattern for quantifiers such as '*' or '{,}' which allow to skip subpattern
        int copies = max(min, 1);
        ImmutableList.Builder<RowPatternNFA> builder = ImmutableList.builder();
        for (int i = 0; i < copies; i++) {
            builder.add(process(node.getPattern(), inExclusion));
        }
        List<RowPatternNFA> parts = builder.build();
        for (int i = 0; i < parts.size() - 1; i++) {
            parts.get(i).getEndState().withTransitions(singleEmptyTransition(parts.get(i + 1).getStartState()));
        }
        State start = new State(singleEmptyTransition(parts.get(0).getStartState()));
        State end = new State();
        RowPatternNFA last = parts.get(parts.size() - 1);
        Transition endingTransition = new Transition(EMPTY, end);
        Transition loopingTransition = new Transition(EMPTY, last.getStartState());

        // in order of preference
        LinkedList<Transition> orderedTransitions = new LinkedList<>();
        if (node.getPatternQuantifier().isGreedy()) {
            orderedTransitions.add(loopingTransition);
            orderedTransitions.add(endingTransition);
        }
        else {
            orderedTransitions.add(endingTransition);
            orderedTransitions.add(loopingTransition);
        }
        last.getEndState().withTransitions(orderedTransitions);

        // for '*' or '{,}' quantifier, add transition to skip the subpattern
        if (min == 0) {
            if (node.getPatternQuantifier().isGreedy()) {
                start.addLeastPreferred(endingTransition);
            }
            else {
                start.addMostPreferred(endingTransition);
            }
        }
        return new RowPatternNFA(start, end);
    }

    private RowPatternNFA rangeQuantified(QuantifiedPattern node, int min, int max, boolean inExclusion)
    {
        checkArgument(min <= max, "invalid range");

        // chain max copies of the subpattern
        ImmutableList.Builder<RowPatternNFA> builder = ImmutableList.builder();
        for (int i = 0; i < max; i++) {
            builder.add(process(node.getPattern(), inExclusion));
        }
        List<RowPatternNFA> parts = builder.build();
        for (int i = 0; i < parts.size() - 1; i++) {
            parts.get(i).getEndState().withTransitions(singleEmptyTransition(parts.get(i + 1).getStartState()));
        }
        State start = new State(singleEmptyTransition(parts.get(0).getStartState()));
        State end = new State();
        parts.get(parts.size() - 1).getEndState().withTransitions(singleEmptyTransition(end));
        Transition endingTransition = new Transition(EMPTY, end);

        // add transitions allowing to skip the remaining part after min iterations
        if (node.getPatternQuantifier().isGreedy()) {
            for (int i = min; i < max; i++) {
                parts.get(i).getStartState().addLeastPreferred(endingTransition);
            }
        }
        else {
            for (int i = min; i < max; i++) {
                parts.get(i).getStartState().addMostPreferred(endingTransition);
            }
        }

        return new RowPatternNFA(start, end);
    }

    private LinkedList<Transition> singleEmptyTransition(State target)
    {
        return singleTransition(new Transition(EMPTY, target));
    }

    private LinkedList<Transition> singleTransition(Transition transition)
    {
        return new LinkedList<>(ImmutableList.of(transition));
    }
}
