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
import io.trino.sql.planner.RowPatternMatcher.MatchResult;
import io.trino.sql.tree.Label;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Set;

import static io.trino.sql.planner.RowPatternMatcher.MatchResult.exclusions;
import static io.trino.sql.tree.Label.EMPTY;
import static io.trino.sql.tree.Label.PARTITION_END;
import static io.trino.sql.tree.Label.PARTITION_START;
import static java.util.Objects.requireNonNull;

public final class RowPatternNFA
{
    private final State startState;
    private final State endState;

    public RowPatternNFA(State startState, State endState)
    {
        this.startState = startState;
        this.endState = endState;
    }

    public State getStartState()
    {
        return startState;
    }

    public State getEndState()
    {
        return endState;
    }

    /**
     * Both iterative and recursive pattern matching approach use the concept of "empty path" to avoid infinite loop.
     * emptyPath is a set of states from which the current state was reached using only empty transitions, plus the current state.
     * NFA must not follow an empty transition leading to either of those states. If it does, the graph exploration from that state
     * will be repeated, leading eventually (regarding transitions preference) to entering the loop again. Since no input row is
     * ingested when following empty transitions, the algorithm would loop infinitely.
     * note: an empty transition closing an empty cycle must not be removed from the graph. When the algorithm is forced out of
     * the loop, it might still follow this transition after backtracking and choosing a different (non-empty) branch on the way.
     */
    public MatchResult findMatch(LabelEvaluator labelEvaluator)
    {
        Stack<Label> labels = new Stack<>();
        Deque<Integer> exclusions = new ArrayDeque<>();

        // boolean matched = findMatchRecursive(startState, 0, labels, exclusions, labelEvaluator, ImmutableSet.of(startState));
        boolean matched = findMatchIterative(labels, exclusions, labelEvaluator);
        if (matched) {
            return new MatchResult(true, labels, exclusions(ImmutableList.copyOf(exclusions)));
        }
        return new MatchResult(false, new Stack<>(), ImmutableList.of());
    }

    private boolean findMatchRecursive(State state, int rowsIngested, Stack<Label> labels, Deque<Integer> exclusions, LabelEvaluator labelEvaluator, Set<State> emptyPath)
    {
        addExclusions(exclusions, state, rowsIngested);

        if (state.equals(endState)) {
            return true;
        }

        for (Transition transition : state.getTransitions()) {
            if (transition.getLabel().equals(EMPTY) && emptyPath.contains(transition.getTarget())) {
                // do not follow a transition if it closes an empty cycle
                // no need to consider PARTITION_START or PARTITION_END labels, because all backward transitions are EMPTY
                continue;
            }
            boolean canFollow = labelEvaluator.evaluateLabel(transition.getLabel(), rowsIngested, labels);
            if (canFollow) {
                boolean addedRow = addLabelIfPresent(labels, transition.getLabel());

                Set<State> updatedEmptyPath;
                if (noRowConsumed(transition.getLabel())) {
                    updatedEmptyPath = ImmutableSet.<State>builder()
                            .addAll(emptyPath)
                            .add(transition.getTarget())
                            .build();
                }
                else {
                    updatedEmptyPath = ImmutableSet.of(transition.getTarget());
                }

                if (findMatchRecursive(transition.getTarget(), rowsIngested + (addedRow ? 1 : 0), labels, exclusions, labelEvaluator, updatedEmptyPath)) {
                    return true;
                }
                if (addedRow) {
                    labels.removeLast();
                }
            }
        }

        removeExclusions(exclusions, state);
        return false;
    }

    // iterative approach, as opposed to recursive approach, avoids the possibility of stack overflow for a very complicated / very long pattern
    private boolean findMatchIterative(Stack<Label> labels, Deque<Integer> exclusions, LabelEvaluator labelEvaluator)
    {
        Deque<IterationState> searchPath = new ArrayDeque<>();
        searchPath.addLast(new IterationState(startState, false));

        while (true) {
            IterationState iterationState = searchPath.peekLast();

            if (iterationState == null) {
                // no more states to explore
                return false;
            }

            State state = iterationState.getState();

            // record exclusions when entering a new state
            if (iterationState.getNextTransition() == 0) {
                addExclusions(exclusions, state, labels.size());
            }

            if (state.equals(endState)) {
                // match found
                return true;
            }

            // pick next transition to follow
            int index = iterationState.getNextTransition();
            Set<State> emptyPath = iterationState.getEmptyPath();
            while (index < state.getTransitions().size()) {
                Transition transition = state.getTransitions().get(index);
                // skip transition if it closes an empty cycle
                // no need to consider PARTITION_START or PARTITION_END labels, because all backward transitions are EMPTY
                if (transition.getLabel().equals(EMPTY) && emptyPath.contains(transition.getTarget())) {
                    index++;
                }
                // skip transition if it is not available in the context of current match
                else if (!labelEvaluator.evaluateLabel(transition.getLabel(), labels.size(), labels)) {
                    index++;
                }
                else {
                    break;
                }
            }

            if (index >= state.getTransitions().size()) {
                // state fully explored, backtrack
                removeExclusions(exclusions, state);
                if (iterationState.isAddedRow()) {
                    labels.removeLast();
                }
                searchPath.removeLast();
            }
            else {
                // follow the transition
                iterationState.setNextTransition(index + 1);

                Transition transition = state.getTransitions().get(index);
                boolean addedRow = addLabelIfPresent(labels, transition.getLabel());

                Set<State> updatedEmptyPath;
                if (noRowConsumed(transition.getLabel())) {
                    updatedEmptyPath = ImmutableSet.<State>builder()
                            .addAll(emptyPath)
                            .add(transition.getTarget())
                            .build();
                }
                else {
                    updatedEmptyPath = ImmutableSet.of(transition.getTarget());
                }
                searchPath.addLast(new IterationState(transition.getTarget(), updatedEmptyPath, addedRow));
            }
        }
    }

    private static boolean addLabelIfPresent(Stack<Label> labels, Label label)
    {
        if (noRowConsumed(label)) {
            return false;
        }
        labels.addLast(label);
        return true;
    }

    private static boolean noRowConsumed(Label label)
    {
        return label.equals(EMPTY) || label.equals(PARTITION_START) || label.equals(PARTITION_END);
    }

    private static void addExclusions(Deque<Integer> exclusions, State state, int rowsIngested)
    {
        if (state.isExclusionStart()) {
            exclusions.addLast(rowsIngested + 1);
        }
        if (state.isExclusionEnd()) {
            exclusions.addLast(rowsIngested + 1);
        }
    }

    private static void removeExclusions(Deque<Integer> exclusions, State state)
    {
        if (state.isExclusionEnd()) {
            exclusions.removeLast();
        }
        if (state.isExclusionStart()) {
            exclusions.removeLast();
        }
    }

    private static class IterationState
    {
        private final State state;
        private final Set<State> emptyPath;
        private int nextTransition;
        private final boolean addedRow;

        public IterationState(State state, boolean addedRow)
        {
            this(state, ImmutableSet.of(state), addedRow);
        }

        public IterationState(State state, Set<State> emptyPath, boolean addedRow)
        {
            this.state = requireNonNull(state, "state is null");
            this.emptyPath = ImmutableSet.copyOf(requireNonNull(emptyPath, "emptyPath is null"));
            this.nextTransition = 0;
            this.addedRow = addedRow;
        }

        public State getState()
        {
            return state;
        }

        public Set<State> getEmptyPath()
        {
            return emptyPath;
        }

        public int getNextTransition()
        {
            return nextTransition;
        }

        public void setNextTransition(int nextTransition)
        {
            this.nextTransition = nextTransition;
        }

        public boolean isAddedRow()
        {
            return addedRow;
        }
    }
}
