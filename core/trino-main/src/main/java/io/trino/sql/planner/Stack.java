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

import java.util.ArrayList;
import java.util.Iterator;

import static com.google.common.collect.Lists.reverse;
import static java.util.Objects.requireNonNull;

public class Stack<E>
{
    private final ArrayList<E> stack;

    public Stack()
    {
        this(new ArrayList<>());
    }

    private Stack(ArrayList<E> stack)
    {
        this.stack = requireNonNull(stack, "stack is null");
    }

    public int size()
    {
        return stack.size();
    }

    public boolean isEmpty()
    {
        return stack.isEmpty();
    }

    public E get(int position)
    {
        return stack.get(position);
    }

    public void addLast(E element)
    {
        stack.add(element);
    }

    public E removeLast()
    {
        return stack.remove(stack.size() - 1);
    }

    public Iterator<E> iterator()
    {
        return stack.iterator();
    }

    public Iterator<E> descendingIterator()
    {
        return reverse(stack).iterator();
    }

    public Stack<E> prefix(int endExclusive)
    {
        return new Stack<E>(new ArrayList<>(stack.subList(0, endExclusive)));
    }
}
