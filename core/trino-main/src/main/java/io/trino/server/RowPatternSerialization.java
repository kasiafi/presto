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
package io.trino.server;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.trino.sql.RowPatternFormatter;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Label;
import io.trino.sql.tree.PatternLabel;
import io.trino.sql.tree.PatternVariable;
import io.trino.sql.tree.RowPattern;
import io.trino.sql.tree.RowPatternRewriter;
import io.trino.sql.tree.RowPatternTreeRewriter;

import javax.inject.Inject;

import java.io.IOException;

import static io.trino.sql.tree.RowPatternTreeRewriter.rewriteWith;

public final class RowPatternSerialization
{
    private RowPatternSerialization() {}

    public static class RowPatternSerializer
            extends JsonSerializer<RowPattern>
    {
        @Override
        public void serialize(RowPattern rowPattern, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
                throws IOException
        {
            jsonGenerator.writeString(RowPatternFormatter.formatPattern(rowPattern));
        }
    }

    public static class RowPatternDeserializer
            extends JsonDeserializer<RowPattern>
    {
        private final SqlParser sqlParser;

        @Inject
        public RowPatternDeserializer(SqlParser sqlParser)
        {
            this.sqlParser = sqlParser;
        }

        @Override
        public RowPattern deserialize(JsonParser jsonParser, DeserializationContext context)
                throws IOException
        {
            RowPattern pattern = sqlParser.createRowPattern(jsonParser.readValueAs(String.class));

            // rewrite identifiers to pattern labels
            return rewriteWith(new RowPatternRewriter<>()
            {
                @Override
                public RowPattern rewritePatternVariable(PatternVariable node, Void context, RowPatternTreeRewriter<Void> treeRewriter)
                {
                    return new PatternLabel(Label.from(node.getName()));
                }
            }, pattern);
        }
    }
}
