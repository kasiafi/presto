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
import static java.util.Objects.requireNonNull;

public class PatternRecognitionRelation
        extends Relation
{
    private final Relation input;
    private final List<Expression> partitionBy;
    private final Optional<OrderBy> orderBy;
    private final List<MeasureDefinition> measures;
    private final Optional<RowsPerMatch> rowsPerMatch;
    private final RowPatternCommon rowPatternCommon;

    public PatternRecognitionRelation(Relation input, List<Expression> partitionBy, Optional<OrderBy> orderBy, List<MeasureDefinition> measures, Optional<RowsPerMatch> rowsPerMatch, RowPatternCommon rowPatternCommon)
    {
        this(Optional.empty(), input, partitionBy, orderBy, measures, rowsPerMatch, rowPatternCommon);
    }

    public PatternRecognitionRelation(NodeLocation location, Relation input, List<Expression> partitionBy, Optional<OrderBy> orderBy, List<MeasureDefinition> measures, Optional<RowsPerMatch> rowsPerMatch, RowPatternCommon rowPatternCommon)
    {
        this(Optional.of(location), input, partitionBy, orderBy, measures, rowsPerMatch, rowPatternCommon);
    }

    private PatternRecognitionRelation(Optional<NodeLocation> location, Relation input, List<Expression> partitionBy, Optional<OrderBy> orderBy, List<MeasureDefinition> measures, Optional<RowsPerMatch> rowsPerMatch, RowPatternCommon rowPatternCommon)
    {
        super(location);
        this.input = requireNonNull(input, "input is null");
        this.partitionBy = requireNonNull(partitionBy, "partitionBy is null");
        this.orderBy = requireNonNull(orderBy, "orderBy is null");
        this.measures = requireNonNull(measures, "measures is null");
        this.rowsPerMatch = requireNonNull(rowsPerMatch, "rowsPerMatch is null");
        this.rowPatternCommon = requireNonNull(rowPatternCommon, "rowPatternCommon is null");
    }

    public Relation getInput()
    {
        return input;
    }

    public List<Expression> getPartitionBy()
    {
        return partitionBy;
    }

    public Optional<OrderBy> getOrderBy()
    {
        return orderBy;
    }

    public List<MeasureDefinition> getMeasures()
    {
        return measures;
    }

    public Optional<RowsPerMatch> getRowsPerMatch()
    {
        return rowsPerMatch;
    }

    public RowPatternCommon getRowPatternCommon()
    {
        return rowPatternCommon;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitPatternRecognitionRelation(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> builder = ImmutableList.builder();
        builder.add(input);
        builder.addAll(partitionBy);
        orderBy.ifPresent(builder::add);
        builder.addAll(measures);
        builder.add(rowPatternCommon);

        return builder.build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("input", input)
                .add("partitionBy", partitionBy)
                .add("orderBy", orderBy.orElse(null))
                .add("measures", measures)
                .add("rowsPerMatch", rowsPerMatch.orElse(null))
                .add("rowPatternCommon", rowPatternCommon)
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

        PatternRecognitionRelation that = (PatternRecognitionRelation) o;
        return Objects.equals(input, that.input) &&
                Objects.equals(partitionBy, that.partitionBy) &&
                Objects.equals(orderBy, that.orderBy) &&
                Objects.equals(measures, that.measures) &&
                Objects.equals(rowsPerMatch, that.rowsPerMatch) &&
                Objects.equals(rowPatternCommon, that.rowPatternCommon);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(input, partitionBy, orderBy, measures, rowsPerMatch, rowPatternCommon);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        return rowsPerMatch.equals(((PatternRecognitionRelation) other).rowsPerMatch);
    }

    public enum RowsPerMatch
    {
        // default
        ONE {
            @Override
            public boolean isOneRow()
            {
                return true;
            }

            @Override
            public boolean isEmptyMatches()
            {
                return true;
            }

            @Override
            public boolean isUnmatchedRows()
            {
                return false;
            }
        },

        ALL_SHOW_EMPTY {
            @Override
            public boolean isOneRow()
            {
                return false;
            }

            @Override
            public boolean isEmptyMatches()
            {
                return true;
            }

            @Override
            public boolean isUnmatchedRows()
            {
                return false;
            }
        },

        ALL_OMIT_EMPTY {
            @Override
            public boolean isOneRow()
            {
                return false;
            }

            @Override
            public boolean isEmptyMatches()
            {
                return false;
            }

            @Override
            public boolean isUnmatchedRows()
            {
                return false;
            }
        },

        ALL_WITH_UNMATCHED {
            @Override
            public boolean isOneRow()
            {
                return false;
            }

            @Override
            public boolean isEmptyMatches()
            {
                return true;
            }

            @Override
            public boolean isUnmatchedRows()
            {
                return true;
            }
        },

        WINDOW {
            @Override
            public boolean isOneRow()
            {
                return true;
            }

            @Override
            public boolean isEmptyMatches()
            {
                return true;
            }

            @Override
            public boolean isUnmatchedRows()
            {
                return true;
            }
        };

        public abstract boolean isOneRow();

        public abstract boolean isEmptyMatches();

        public abstract boolean isUnmatchedRows();
    }
}
