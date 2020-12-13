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

import io.trino.sql.tree.Label;

import java.util.Iterator;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class LogicalIndexPointer
{
    // logical position is a position among rows tagged with certain label (or label from a certain set)
    // it has the following semantics:
    // start from FIRST or LAST row tagged with the label (with RUNNING or FINAL semantics), and go logicalOffset steps forward (for FIRST) or backward (for LAST),
    // skipping to consecutive rows with matching label
    // Default: RUNNING LAST offset = 0
    private final Set<Label> label;
    private final boolean last;
    private final boolean running;
    private final int logicalOffset;

    // physical offset is the offset in physical rows, starting from the logical position. negative for PREV, positive for NEXT. The default is -1 for PREV and 1 for NEXT.
    // Unspecified physical offset defaults to 0.
    private final int physicalOffset;

    public LogicalIndexPointer(Set<Label> label, boolean last, boolean running, int logicalOffset, int physicalOffset)
    {
        this.label = requireNonNull(label, "label is null");
        this.last = last;
        this.running = running;
        checkArgument(logicalOffset >= 0, "logical offset must be >= 0, actual: " + logicalOffset);
        this.logicalOffset = logicalOffset;
        this.physicalOffset = physicalOffset;
    }

    public OptionalInt resolvePosition(Label newLabel, int matchedRowsCount, Stack<Label> matchedLabels, int partitionStart, int partitionEnd, int patternStart)
    {
        // During label evaluation, the new label is considered already assigned.
        // It is appended while resolving physical position and removed afterwards.
        matchedLabels.addLast(newLabel);

        OptionalInt position;
        if (last) {
            // startSearch points beyond matched rows, so that the currently evaluated label is considered as assigned.
            position = findLastAndBackward(matchedLabels, patternStart + matchedRowsCount + 1);
        }
        else {
            position = findFirstAndForward(matchedLabels, patternStart - 1);
        }
        position = adjustPosition(position, partitionStart, partitionEnd);

        matchedLabels.removeLast();

        return position;
    }

    // LAST(A.price, 3): find the last occurrence of label "A" and go 3 occurrences backward
    private OptionalInt findLastAndBackward(Stack<Label> matchedLabels, int startSearch)
    {
        int found = 0;
        int position = startSearch;
        Iterator<Label> backward = matchedLabels.descendingIterator();
        while (backward.hasNext() && found <= logicalOffset) {
            Label current = backward.next();
            position--;
            if (label.isEmpty() || label.contains(current)) { // empty label denotes "universal row pattern variable", which always matches
                found++;
            }
        }
        if (found == logicalOffset + 1) {
            return OptionalInt.of(position);
        }
        return OptionalInt.empty();
    }

    // FIRST(A.price, 3): find the first occurrence of label "A" and go 3 occurrences forward
    private OptionalInt findFirstAndForward(Stack<Label> matchedLabels, int startSearch)
    {
        int found = 0;
        int position = startSearch;
        Iterator<Label> forward = matchedLabels.iterator();
        while (forward.hasNext() && found <= logicalOffset) {
            Label current = forward.next();
            position++;
            if (label.isEmpty() || label.contains(current)) { // empty label denotes "universal row pattern variable", which always matches
                found++;
            }
        }
        if (found == logicalOffset + 1) {
            return OptionalInt.of(position);
        }
        return OptionalInt.empty();
    }

    // adjust position by physical offset: skip a certain number of rows, regardless of labels
    // check if the new position is within partition bound by: partitionStart - inclusive, partitionEnd - exclusive
    private OptionalInt adjustPosition(OptionalInt position, int partitionStart, int partitionEnd)
    {
        if (position.isEmpty()) {
            return OptionalInt.empty();
        }
        int start = position.getAsInt();
        int target = start + physicalOffset;
        if (target < partitionStart || target >= partitionEnd) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(target);
    }

    // This method is used when computing row pattern measures after finding a match. Stack of matched labels is complete.
    // Search is limited up to the current row in case of running semantics and to the entire match in case of final semantics.
    public OptionalInt resolvePosition(int currentRow, int matchedRowsCount, Stack<Label> matchedLabels, int partitionStart, int partitionEnd, int patternStart)
    {
        checkArgument(currentRow >= patternStart && currentRow < patternStart + matchedRowsCount, "current row is out of bounds of the match");
        OptionalInt position;

        Stack<Label> runningMatchedLabels = new Stack<>();
        if (running) {
            runningMatchedLabels = matchedLabels.prefix(currentRow - patternStart + 1);
        }

        if (last && running) {
            position = findLastAndBackward(runningMatchedLabels, currentRow + 1);
        }
        else if (last) {
            position = findLastAndBackward(matchedLabels, patternStart + matchedRowsCount);
        }
        else if (running) {
            position = findFirstAndForward(runningMatchedLabels, patternStart - 1);
        }
        else {
            position = findFirstAndForward(matchedLabels, patternStart - 1);
        }
        position = adjustPosition(position, partitionStart, partitionEnd);

        return position;
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

        LogicalIndexPointer that = (LogicalIndexPointer) o;
        return last == that.last &&
                running == that.running &&
                logicalOffset == that.logicalOffset &&
                physicalOffset == that.physicalOffset &&
                label.equals(that.label);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(label, last, running, logicalOffset, physicalOffset);
    }
}
