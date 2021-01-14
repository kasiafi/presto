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
import io.trino.sql.tree.ProcessingMode.Mode;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PatternNavigationFunction
        extends Expression
{
    private final Type type;
    private final Expression argument;
    private final long offset;
    private final Optional<Mode> processingMode;

    public PatternNavigationFunction(Type type, List<Expression> arguments, Optional<ProcessingMode> processingMode)
    {
        super(Optional.empty());
        this.type = requireNonNull(type, "type is null");
        checkArgument(arguments.size() > 0 && arguments.size() <= 2, "invalid arguments number: %s. Expected 1 or 2 arguments" + arguments.size());
        this.argument = requireNonNull(arguments.get(0), "argument is null");
        if (arguments.size() == 2) {
            this.offset = ((LongLiteral) arguments.get(1)).getValue(); //TODO record it in the analysis?
        }
        else {
            switch (type) {
                case FIRST:
                case LAST:
                    this.offset = 0;
                    break;
                case PREV:
                case NEXT:
                    this.offset = 1;
                    break;
                default:
                    throw new IllegalStateException("unsupported pattern navigation function type " + type);
            }
        }
        this.processingMode = requireNonNull(processingMode, "processingMode is null").map(ProcessingMode::getMode);
    }

    public PatternNavigationFunction(Type type, Expression argument, long offset, Optional<Mode> processingMode)
    {
        super(Optional.empty());
        this.type = requireNonNull(type, "type is null");
        this.argument = requireNonNull(argument, "argument is null");
        checkArgument(offset >= 0, "pattern navigation offset must not be negative (actual: %s)", offset);
        this.offset = offset;
        this.processingMode = requireNonNull(processingMode, "processingMode is null");
    }

    public Type getType()
    {
        return type;
    }

    public Expression getArgument()
    {
        return argument;
    }

    public long getOffset()
    {
        return offset;
    }

    public Optional<Mode> getProcessingMode()
    {
        return processingMode;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitPatternNavigationFunction(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(argument);
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
        PatternNavigationFunction that = (PatternNavigationFunction) o;
        return Objects.equals(type, that.type) &&
                Objects.equals(argument, that.argument) &&
                Objects.equals(offset, that.offset) &&
                Objects.equals(processingMode, that.processingMode);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, argument, offset, processingMode);
    }

    public enum Type
    {
        FIRST,
        LAST,
        PREV,
        NEXT;

        public static Type from(String name)
        {
            switch (name) {
                case "FIRST":
                    return FIRST;
                case "LAST":
                    return LAST;
                case "PREV":
                    return PREV;
                case "NEXT":
                    return NEXT;
                default:
                    throw new IllegalArgumentException("unsupported pattern navigation function type " + name);
            }
        }
    }
}
