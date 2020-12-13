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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class RowPatternCommon
        extends Node
{
    private final Optional<SkipTo> afterMatchSkipTo;
    private final Optional<Boolean> initial;
    private final RowPattern pattern;
    private final List<SubsetDefinition> subsets;
    private final List<VariableDefinition> variableDefinitions;

    public RowPatternCommon(Optional<SkipTo> afterMatchSkipTo, Optional<Boolean> initial, RowPattern pattern, List<SubsetDefinition> subsets, List<VariableDefinition> variableDefinitions)
    {
        this(Optional.empty(), afterMatchSkipTo, initial, pattern, subsets, variableDefinitions);
    }

    public RowPatternCommon(NodeLocation location, Optional<SkipTo> afterMatchSkipTo, Optional<Boolean> initial, RowPattern pattern, List<SubsetDefinition> subsets, List<VariableDefinition> variableDefinitions)
    {
        this(Optional.of(location), afterMatchSkipTo, initial, pattern, subsets, variableDefinitions);
    }

    private RowPatternCommon(Optional<NodeLocation> location, Optional<SkipTo> afterMatchSkipTo, Optional<Boolean> initial, RowPattern pattern, List<SubsetDefinition> subsets, List<VariableDefinition> variableDefinitions)
    {
        super(location);
        this.afterMatchSkipTo = requireNonNull(afterMatchSkipTo, "afterMatchSkipTo is null");
        this.initial = requireNonNull(initial, "initial is null");
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.subsets = requireNonNull(subsets, "subsets is null");
        requireNonNull(variableDefinitions, "variableDefinitions is null");
        checkArgument(!variableDefinitions.isEmpty(), "variableDefinitions is empty");
        this.variableDefinitions = variableDefinitions;
    }

    public Optional<SkipTo> getAfterMatchSkipTo()
    {
        return afterMatchSkipTo;
    }

    public Optional<Boolean> getInitial()
    {
        return initial;
    }

    public RowPattern getPattern()
    {
        return pattern;
    }

    public List<SubsetDefinition> getSubsets()
    {
        return subsets;
    }

    public List<VariableDefinition> getVariableDefinitions()
    {
        return variableDefinitions;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRowPatternCommon(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> builder = ImmutableList.builder();
        afterMatchSkipTo.ifPresent(builder::add);
        builder.add(pattern)
                .addAll(subsets)
                .addAll(variableDefinitions);

        return builder.build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("afterMatchSkipTo", afterMatchSkipTo)
                .add("initial", initial.orElse(null))
                .add("pattern", pattern)
                .add("subsets", subsets)
                .add("variableDefinitions", variableDefinitions)
                .omitNullValues()
                .toString();
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

        RowPatternCommon that = (RowPatternCommon) o;
        return Objects.equals(afterMatchSkipTo, that.afterMatchSkipTo) &&
                Objects.equals(initial, that.initial) &&
                Objects.equals(pattern, that.pattern) &&
                Objects.equals(subsets, that.subsets) &&
                Objects.equals(variableDefinitions, that.variableDefinitions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(afterMatchSkipTo, initial, pattern, subsets, variableDefinitions);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        return Objects.equals(initial, ((RowPatternCommon) other).initial);
    }
}
