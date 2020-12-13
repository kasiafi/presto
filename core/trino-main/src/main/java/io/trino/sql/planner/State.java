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

import java.util.LinkedList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class State
{
    // List of transitions ordered by preference.
    // Mutable. No copy on construction. Reuse carefully.
    private LinkedList<Transition> transitions;

    private boolean isExclusionStart;
    private boolean isExclusionEnd;

    public State()
    {
        this(new LinkedList<>());
    }

    public State(LinkedList<Transition> transitions)
    {
        this(transitions, false, false);
    }

    public State(LinkedList<Transition> transitions, boolean isExclusionStart, boolean isExclusionEnd)
    {
        this.transitions = requireNonNull(transitions, "transitions is null");
        this.isExclusionStart = isExclusionStart;
        this.isExclusionEnd = isExclusionEnd;
    }

    public List<Transition> getTransitions()
    {
        return ImmutableList.copyOf(transitions);
    }

    public boolean isExclusionStart()
    {
        return isExclusionStart;
    }

    public boolean isExclusionEnd()
    {
        return isExclusionEnd;
    }

    public void withTransitions(LinkedList<Transition> transitions)
    {
        checkState(this.transitions.isEmpty(), "overriding non-empty transitions list");
        this.transitions = requireNonNull(transitions, "transitions is null");
    }

    public void addLeastPreferred(Transition transition)
    {
        transitions.add(transition);
    }

    public void addMostPreferred(Transition transition)
    {
        transitions.add(0, transition);
    }
}
