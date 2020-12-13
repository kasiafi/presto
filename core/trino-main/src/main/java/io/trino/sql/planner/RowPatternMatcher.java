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
import io.trino.sql.tree.Label;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class RowPatternMatcher
{
    private final RowPatternNFA rowPatternNFA;
    private final LabelEvaluator labelEvaluator;

    public RowPatternMatcher(RowPatternNFA rowPatternNFA, LabelEvaluator labelEvaluator)
    {
        this.rowPatternNFA = requireNonNull(rowPatternNFA, "rowPatternNFA is null");
        this.labelEvaluator = requireNonNull(labelEvaluator, "labelEvaluator is null");
    }

    public MatchResult findMatch()
    {
        return rowPatternNFA.findMatch(labelEvaluator);
    }

    public static class MatchResult
    {
        boolean matched;
        Stack<Label> labels;

        // list of exclusions relative to matched row sequence
        // exclusion (x, y) applies to x-th (inclusive) up to y-th (exclusive) row of the match, starting at 1
        List<Exclusion> exclusions;

        public MatchResult(boolean matched, Stack<Label> labels, List<Exclusion> exclusions)
        {
            requireNonNull(labels, "labels is null");
            requireNonNull(exclusions, "exclusions is null");
            checkArgument(matched || labels.isEmpty() && exclusions.isEmpty(), "cannot have labels or exclusions in empty match");
            for (Exclusion exclusion : exclusions) {
                checkArgument(exclusion.start <= labels.size() && exclusion.end <= labels.size() + 1, "exclusion out of match bounds");
            }

            this.matched = matched;
            this.labels = labels;
            this.exclusions = exclusions;
        }

        public static List<Exclusion> exclusions(List<Integer> bounds)
        {
            checkArgument(bounds.size() % 2 == 0, "mismatched bounds");
            ImmutableList.Builder<Exclusion> builder = ImmutableList.builder();
            for (int i = 0; i < bounds.size(); i = i + 2) {
                builder.add(new Exclusion(bounds.get(i), bounds.get(i + 1)));
            }
            return builder.build();
        }

        public boolean isMatched()
        {
            return matched;
        }

        public Stack<Label> getLabels()
        {
            return labels;
        }

        public List<Exclusion> getExclusions()
        {
            return exclusions;
        }
    }

    public static class Exclusion
    {
        // inclusive - the first row to be excluded from the result
        int start;

        // exclusive - the first row not to be excluded from the result
        int end;

        public Exclusion(int start, int end)
        {
            checkArgument(start >= 1 && end >= 1, "row pattern exclusion bound must be positive");
            checkArgument(start <= end, "row pattern exclusion start must be lower or equal than end");
            this.start = start;
            this.end = end;
        }

        public int getStart()
        {
            return start;
        }

        public int getEnd()
        {
            return end;
        }
    }
}
