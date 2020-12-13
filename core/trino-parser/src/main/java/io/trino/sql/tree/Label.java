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

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class Label
{
    public static final Label EMPTY = new Label("");
    public static final Label PARTITION_START = new Label("^");
    public static final Label PARTITION_END = new Label("$");

    private final String name;

    public static Label from(Identifier identifier)
    {
        requireNonNull(identifier, "identifier is null");
        String name = identifier.getValue().toUpperCase(ENGLISH);
        checkState(!name.equals("^") && !name.equals("$") && !name.equals(""), "cannot create label from identifier: " + name);
        return new Label(name);
    }

    public static Optional<Label> tryCreateFrom(Identifier identifier)
    {
        requireNonNull(identifier, "identifier is null");
        String name = identifier.getValue().toUpperCase(ENGLISH);
        if (name.equals("^") || name.equals("$") || name.equals("")) {
            return Optional.empty();
        }
        return Optional.of(new Label(name));
    }

    private Label(String name)
    {
        this.name = requireNonNull(name, "name is null");
    }

    public String getName()
    {
        return name;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Label o = (Label) obj;
        return Objects.equals(name, o.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name);
    }
}
