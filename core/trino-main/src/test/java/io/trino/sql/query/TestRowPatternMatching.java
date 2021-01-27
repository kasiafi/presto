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
package io.trino.sql.query;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRowPatternMatching
{
    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    // TODO refactor ot only pass the pattern
    public void testSimplePattern()
    {
        assertThat(assertions.query("SELECT M.Symbol, /* ticker symbol */ " +
                "                 M.Tradeday, /* ordering column */ " +
                "                 M.Startp, /* starting price */ " +
                "                 M.Bottomp, /* bottom price */ " +
                "                 M.Endp /* ending price */ " +
                "          FROM (VALUES " +
                "                   (1, 1, 100), " +
                "                   (1, 2, 8), " +
                "                   (1, 3, 6), " +
                "                   (1, 4, 8), " +
                "                   (1, 5, 10), " +
                "                   (1, 6, 10), " +
                "                   (1, 7, 11), " +
                "                   (1, 8, 9), " +
                "                   (1, 9, 8), " +
                "                   (1, 10, 10) " +
                "               ) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY Symbol " +
                "                   ORDER BY Tradeday " +
                "                   MEASURES " +
                "                            A.Price AS Startp, " +
                "                            LAST (B.Price) AS Bottomp, " +
                "                            LAST (C.Price) AS Endp " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN ( A {- B+ C+ {- A A -} B+ C+ -} ) " +
                "                   SUBSET U = (A, B, C) " +
                "                   DEFINE /* A defaults to True, matches any row */ " +
                "                          B AS B.Price < PREV (B.Price), " +
                "                          C AS C.Price > PREV (C.Price) " +
                "                ) AS M"))
                .matches("VALUES (1, 1, 100, CAST(NULL AS integer), CAST(NULL AS integer))");

        assertThat(assertions.query("SELECT M.Symbol, /* ticker symbol */ " +
                "                 M.Tradeday, /* ordering column */ " +
                "                 M.Startp /* starting price */ " +
                "          FROM (VALUES " +
                "                   (1, 1, 100), " +
                "                   (1, 2, 8), " +
                "                   (1, 3, 6), " +
                "                   (1, 4, 8), " +
                "                   (1, 5, 10), " +
                "                   (1, 6, 10), " +
                "                   (1, 7, 11), " +
                "                   (1, 8, 9), " +
                "                   (1, 9, 8), " +
                "                   (1, 10, 10) " +
                "               ) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY Symbol " +
                "                   ORDER BY Tradeday " +
                "                   MEASURES A.Price AS Startp " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN ( A ()* ) " +
                "                   DEFINE A AS true " +
                "                ) AS M"))
                .matches("VALUES " +
                        "     (1,  1, 100)," +
                        "     (1,  2,   8)," +
                        "     (1,  3,   6)," +
                        "     (1,  4,   8)," +
                        "     (1,  5,  10)," +
                        "     (1,  6,  10)," +
                        "     (1,  7,  11)," +
                        "     (1,  8,   9)," +
                        "     (1,  9,   8)," +
                        "     (1, 10,  10)");

        assertThat(assertions.query("SELECT M.Symbol, /* ticker symbol */ " +
                "                 M.Tradeday, /* ordering column */ " +
                "                 M.Startp, /* starting price */ " +
                "                 M.Bottomp, /* bottom price */ " +
                "                 M.Endp /* ending price */ " +
                "          FROM (VALUES " +
                "                   (1, 1, 100), " +
                "                   (1, 2, 8), " +
                "                   (1, 3, 6), " +
                "                   (1, 4, 8), " +
                "                   (1, 5, 10), " +
                "                   (1, 6, 10), " +
                "                   (1, 7, 11), " +
                "                   (1, 8, 9), " +
                "                   (1, 9, 8), " +
                "                   (1, 10, 10) " +
                "               ) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY Symbol " +
                "                   ORDER BY Tradeday " +
                "                   MEASURES " +
                "                            A.Price AS Startp, " +
                "                            LAST (B.Price) AS Bottomp, " +
                "                            LAST (C.Price) AS Endp " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN ( A ()* B+ C+ ) " +
                "                   SUBSET U = (A, B, C) " +
                "                   DEFINE /* A defaults to True, matches any row */ " +
                "                          B AS B.Price < PREV (B.Price), " +
                "                          C AS C.Price > PREV (C.Price) " +
                "                ) AS M"))
                .matches("VALUES " +
                        "     (1,  1, 100, CAST(NULL AS integer), CAST(NULL AS integer))," +
                        "     (1,  2, 100,    8, NULL)," +
                        "     (1,  3, 100,    6, NULL)," +
                        "     (1,  4, 100,    6,    8)," +
                        "     (1,  5, 100,    6,   10)," +
                        "     (1,  7,  11, NULL, NULL)," +
                        "     (1,  8,  11,    9, NULL)," +
                        "     (1,  9,  11,    8, NULL)," +
                        "     (1, 10,  11,    8,   10)");

        assertThat(assertions.query("SELECT M.Symbol, /* ticker symbol */ " +
                "                 M.Tradeday, /* ordering column */ " +
                "                 M.Startp, /* starting price */ " +
                "                 M.Bottomp, /* bottom price */ " +
                "                 M.Endp /* ending price */ " +
                "          FROM (VALUES " +
                "                   (1, 1, 100), " +
                "                   (1, 2, 8), " +
                "                   (1, 3, 6), " +
                "                   (1, 4, 8), " +
                "                   (1, 5, 10), " +
                "                   (1, 6, 10), " +
                "                   (1, 7, 11), " +
                "                   (1, 8, 9), " +
                "                   (1, 9, 8), " +
                "                   (1, 10, 10) " +
                "               ) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY Symbol " +
                "                   ORDER BY Tradeday " +
                "                   MEASURES " +
                "                            A.Price AS Startp, " +
                "                            LAST (B.Price) AS Bottomp, " +
                "                            LAST (C.Price) AS Endp " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN ( A (()* | B+) C+) " +
                "                   SUBSET U = (A, B, C) " +
                "                   DEFINE /* A defaults to True, matches any row */ " +
                "                          B AS B.Price < PREV (B.Price), " +
                "                          C AS C.Price > PREV (C.Price) " +
                "                ) AS M"))
                .matches("VALUES " +
                        "     (1,  1, 100, CAST(NULL AS integer), CAST(NULL AS integer))," +
                        "     (1,  2, 100,    8, NULL)," +
                        "     (1,  3, 100,    6, NULL)," +
                        "     (1,  4, 100,    6,    8)," +
                        "     (1,  5, 100,    6,   10)," +
                        "     (1,  6,  10, NULL, NULL)," +
                        "     (1,  7,  10, NULL,   11)," +
                        "     (1,  8,   9, NULL, NULL)," +
                        "     (1,  9,   9,    8, NULL)," +
                        "     (1, 10,   9,    8,   10)");

        assertThat(assertions.query("SELECT M.Symbol, /* ticker symbol */ " +
                "                 M.Tradeday, /* ordering column */ " +
                "                 M.Startp, /* starting price */ " +
                "                 M.Bottomp, /* bottom price */ " +
                "                 M.Endp /* ending price */ " +
                "          FROM (VALUES " +
                "                   (1, 1, 100), " +
                "                   (1, 2, 8), " +
                "                   (1, 3, 6), " +
                "                   (1, 4, 8), " +
                "                   (1, 5, 10), " +
                "                   (1, 6, 10), " +
                "                   (1, 7, 11), " +
                "                   (1, 8, 9), " +
                "                   (1, 9, 8), " +
                "                   (1, 10, 10) " +
                "               ) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY Symbol " +
                "                   ORDER BY Tradeday " +
                "                   MEASURES " +
                "                            A.Price AS Startp, " +
                "                            LAST (B.Price) AS Bottomp, " +
                "                            LAST (C.Price) AS Endp " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN ( A {- B+ C+ -} A A {- B+ C+ -} ) " +
                "                   SUBSET U = (A, B, C) " +
                "                   DEFINE /* A defaults to True, matches any row */ " +
                "                          B AS B.Price < PREV (B.Price), " +
                "                          C AS C.Price > PREV (C.Price) " +
                "                ) AS M"))
                .matches("VALUES " +
                        "     (1,  1, 100, CAST(NULL AS integer), CAST(NULL AS integer))," +
                        "     (1,  6, 10,  6, 10)," +
                        "     (1,  7, 11,  6, 10)");

        assertThat(assertions.query("SELECT M.Symbol, /* ticker symbol */ " +
                "                 M.Tradeday, /* ordering column */ " +
                "                 M.Startp, /* starting price */ " +
                "                 M.Bottomp, /* bottom price */ " +
                "                 M.Endp /* ending price */ " +
                "          FROM (VALUES " +
                "                   (1, 1, 100), " +
                "                   (1, 2, 8), " +
                "                   (1, 3, 6), " +
                "                   (1, 4, 8), " +
                "                   (1, 5, 10), " +
                "                   (1, 6, 10), " +
                "                   (1, 7, 11), " +
                "                   (1, 8, 9), " +
                "                   (1, 9, 8), " +
                "                   (1, 10, 10) " +
                "               ) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY Symbol " +
                "                   ORDER BY Tradeday " +
                "                   MEASURES " +
                "                            A.Price AS Startp, " +
                "                            LAST (B.Price) AS Bottomp, " +
                "                            LAST (C.Price) AS Endp " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN ( A {- B+ C+ -} A {- B+ C+ -} ) " +
                "                   SUBSET U = (A, B, C) " +
                "                   DEFINE /* A defaults to True, matches any row */ " +
                "                          B AS B.Price < PREV (B.Price), " +
                "                          C AS C.Price > PREV (C.Price) " +
                "                ) AS M"))
                .returnsEmptyResult();

        assertThat(assertions.query("SELECT M.Symbol, /* ticker symbol */ " +
                "                 M.Tradeday, /* ordering column */ " +
                "                 M.Startp, /* starting price */ " +
                "                 M.Bottomp, /* bottom price */ " +
                "                 M.Endp /* ending price */ " +
                "          FROM (VALUES " +
                "                   (1, 1, 100), " +
                "                   (1, 2, 8), " +
                "                   (1, 3, 6), " +
                "                   (1, 4, 8), " +
                "                   (1, 5, 10), " +
                "                   (1, 6, 10), " +
                "                   (1, 7, 11), " +
                "                   (1, 8, 9), " +
                "                   (1, 9, 8), " +
                "                   (1, 10, 10) " +
                "               ) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY Symbol " +
                "                   ORDER BY Tradeday " +
                "                   MEASURES " +
                "                            A.Price AS Startp, " +
                "                            LAST (B.Price) AS Bottomp, " +
                "                            LAST (C.Price) AS Endp " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN ( A {- B+ C+ -}) " +
                "                   SUBSET U = (A, B, C) " +
                "                   DEFINE /* A defaults to True, matches any row */ " +
                "                          B AS B.Price < PREV (B.Price), " +
                "                          C AS C.Price > PREV (C.Price) " +
                "                ) AS M"))
                .matches("VALUES " +
                        "     (1,  1, 100, CAST(NULL AS integer), CAST(NULL AS integer))," +
                        "     (1,  7,  11, NULL, NULL)");

        assertThat(assertions.query("SELECT M.Symbol, /* ticker symbol */ " +
                "                 M.Tradeday, /* ordering column */ " +
                "                 M.Startp /* starting price */ " +
                "          FROM (VALUES " +
                "                   (1, 1, 100), " +
                "                   (1, 2, 8), " +
                "                   (1, 3, 6), " +
                "                   (1, 4, 8), " +
                "                   (1, 5, 10), " +
                "                   (1, 6, 10), " +
                "                   (1, 7, 11), " +
                "                   (1, 8, 9), " +
                "                   (1, 9, 8), " +
                "                   (1, 10, 10) " +
                "               ) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY Symbol " +
                "                   ORDER BY Tradeday " +
                "                   MEASURES A.Price AS Startp " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN ( A {- () -} A) " +
                "                   DEFINE A AS true " +
                "                ) AS M"))
                .matches("VALUES " +
                        "     (1,  1, 100)," +
                        "     (1,  2,   8)," +
                        "     (1,  3,   6)," +
                        "     (1,  4,   8)," +
                        "     (1,  5,  10)," +
                        "     (1,  6,  10)," +
                        "     (1,  7,  11)," +
                        "     (1,  8,   9)," +
                        "     (1,  9,   8)," +
                        "     (1, 10,  10)");

        assertThat(assertions.query("SELECT M.Symbol, /* ticker symbol */ " +
                "                 M.Tradeday, /* ordering column */ " +
                "                 M.Startp, /* starting price */ " +
                "                 M.Bottomp, /* bottom price */ " +
                "                 M.Endp /* ending price */ " +
                "          FROM (VALUES " +
                "                   (1, 1, 100), " +
                "                   (1, 2, 8), " +
                "                   (1, 3, 6), " +
                "                   (1, 4, 8), " +
                "                   (1, 5, 10), " +
                "                   (1, 6, 10), " +
                "                   (1, 7, 11), " +
                "                   (1, 8, 9), " +
                "                   (1, 9, 8), " +
                "                   (1, 10, 10) " +
                "               ) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY Symbol " +
                "                   ORDER BY Tradeday " +
                "                   MEASURES " +
                "                            A.Price AS Startp, " +
                "                            LAST (B.Price) AS Bottomp, " +
                "                            LAST (C.Price) AS Endp " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN ( A B+ C+ $) " +
                "                   SUBSET U = (A, B, C) " +
                "                   DEFINE /* A defaults to True, matches any row */ " +
                "                          B AS B.Price < PREV (B.Price), " +
                "                          C AS C.Price > PREV (C.Price) " +
                "                ) AS M"))
                .matches("VALUES " +
                        "     (1,  7, 11, CAST(NULL AS integer), CAST(NULL AS integer))," +
                        "     (1,  8, 11,  9, NULL)," +
                        "     (1,  9, 11,  8, NULL)," +
                        "     (1, 10, 11,  8,   10)");

        assertThat(assertions.query("SELECT M.Symbol, /* ticker symbol */ " +
                "                 M.Tradeday, /* ordering column */ " +
                "                 M.Startp, /* starting price */ " +
                "                 M.Bottomp, /* bottom price */ " +
                "                 M.Endp /* ending price */ " +
                "          FROM (VALUES " +
                "                   (1, 1, 100), " +
                "                   (1, 2, 8), " +
                "                   (1, 3, 6), " +
                "                   (1, 4, 8), " +
                "                   (1, 5, 10), " +
                "                   (1, 6, 10), " +
                "                   (1, 7, 11), " +
                "                   (1, 8, 9), " +
                "                   (1, 9, 8), " +
                "                   (1, 10, 10) " +
                "               ) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY Symbol " +
                "                   ORDER BY Tradeday " +
                "                   MEASURES " +
                "                            A.Price AS Startp, " +
                "                            LAST (B.Price) AS Bottomp, " +
                "                            LAST (C.Price) AS Endp " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN (^ A B+ C+ $) " +
                "                   SUBSET U = (A, B, C) " +
                "                   DEFINE /* A defaults to True, matches any row */ " +
                "                          B AS B.Price < PREV (B.Price), " +
                "                          C AS C.Price > PREV (C.Price) " +
                "                ) AS M"))
                .returnsEmptyResult();

        assertThat(assertions.query("SELECT M.Symbol, /* ticker symbol */ " +
                "                 M.Tradeday, /* ordering column */ " +
                "                 M.Startp, /* starting price */ " +
                "                 M.Bottomp, /* bottom price */ " +
                "                 M.Endp /* ending price */ " +
                "          FROM (VALUES " +
                "                   (1, 1, 100), " +
                "                   (1, 2, 8), " +
                "                   (1, 3, 6), " +
                "                   (1, 4, 8), " +
                "                   (1, 5, 10), " +
                "                   (1, 6, 10), " +
                "                   (1, 7, 11), " +
                "                   (1, 8, 9), " +
                "                   (1, 9, 8), " +
                "                   (1, 10, 10) " +
                "               ) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY Symbol " +
                "                   ORDER BY Tradeday " +
                "                   MEASURES " +
                "                            A.Price AS Startp, " +
                "                            LAST (B.Price) AS Bottomp, " +
                "                            LAST (C.Price) AS Endp " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN (^ A B+ C+) " +
                "                   SUBSET U = (A, B, C) " +
                "                   DEFINE /* A defaults to True, matches any row */ " +
                "                          B AS B.Price < PREV (B.Price), " +
                "                          C AS C.Price > PREV (C.Price) " +
                "                ) AS M"))
                .matches("VALUES " +
                        "     (1,  1, 100, CAST(NULL AS integer), CAST(NULL AS integer))," +
                        "     (1,  2, 100,    8, NULL)," +
                        "     (1,  3, 100,    6, NULL)," +
                        "     (1,  4, 100,    6,    8)," +
                        "     (1,  5, 100,    6,   10)");

        assertThat(assertions.query("SELECT M.Symbol, /* ticker symbol */ " +
                "                 M.Matchno, /* match number */ " +
                "                 M.Classy, /* classifier (label) */ " +
                "                 M.Tradeday, /* ordering column */ " +
                "                 M.Startp, /* starting price */ " +
                "                 M.Bottomp, /* bottom price */ " +
                "                 M.Endp /* ending price */ " +
                "          FROM (VALUES " +
                "                   (1, 1, 100), " +
                "                   (1, 2, 8), " +
                "                   (1, 3, 6), " +
                "                   (1, 4, 8), " +
                "                   (1, 5, 10), " +
                "                   (1, 6, 10), " +
                "                   (1, 7, 11), " +
                "                   (1, 8, 9), " +
                "                   (1, 9, 8), " +
                "                   (1, 10, 10) " +
                "               ) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY Symbol " +
                "                   ORDER BY Tradeday " +
                "                   MEASURES " +
                "                            match_number() AS Matchno, " +
                "                            classifier() AS Classy, " +
                "                            A.Price AS Startp, " +
                "                            LAST (B.Price) AS Bottomp, " +
                "                            LAST (C.Price) AS Endp " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN (A B+ C+) " +
                "                   SUBSET U = (A, B, C) " +
                "                   DEFINE /* A defaults to True, matches any row */ " +
                "                          B AS B.Price < PREV (B.Price) AND PREV(classifier()) = 'A' AND match_number() < 3, " +
                "                          C AS C.Price > PREV (C.Price) " +
                "                ) AS M"))
                .matches("VALUES " +
                        "     (1,  CAST(1 AS bigint),  CAST('A' AS varchar),  2,  8, CAST(NULL AS integer), CAST(NULL AS integer))," +
                        "     (1, 1, 'B', 3, 8,    6, NULL)," +
                        "     (1, 1, 'C', 4, 8,    6,    8)," +
                        "     (1, 1, 'C', 5, 8,    6,   10)," +
                        "     (1, 2, 'A', 8, 9, NULL, NULL)," +
                        "     (1, 2, 'B', 9, 9,    8, NULL)," +
                        "     (1, 2, 'C', 10, 9,   8,   10)");

        // empty match
        assertThat(assertions.query("SELECT M.Symbol, /* ticker symbol */ " +
                "                 M.Matchno, /* match number */ " +
                "                 M.Classy, /* classifier (label) */ " +
                "                 M.Startp, /* starting price */ " +
                "                 M.Bottomp, /* bottom price */ " +
                "                 M.Endp /* ending price */ " +
                "          FROM (VALUES " +
                "                   (1, 1, 100), " +
                "                   (1, 2, 8), " +
                "                   (1, 3, 6), " +
                "                   (1, 4, 8), " +
                "                   (1, 5, 10), " +
                "                   (1, 6, 10), " +
                "                   (1, 7, 11), " +
                "                   (1, 8, 9), " +
                "                   (1, 9, 8), " +
                "                   (1, 10, 10) " +
                "               ) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY Symbol " +
                "                   ORDER BY Tradeday " +
                "                   MEASURES " +
                "                            match_number() AS Matchno, " +
                "                            classifier() AS Classy, " +
                "                            A.Price AS Startp, " +
                "                            LAST (A.Price) AS Bottomp, " +
                "                            LAST (A.Price) AS Endp " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN (() | A) " +
                "                   DEFINE A AS true " +
                "                ) AS M"))
                .matches("VALUES " +
                        "     (1,  CAST(1 AS bigint),  CAST(NULL AS varchar), CAST(NULL AS integer), CAST(NULL AS integer), CAST(NULL AS integer))," +
                        "     (1,  2, NULL, NULL, NULL, NULL)," +
                        "     (1,  3, NULL, NULL, NULL, NULL)," +
                        "     (1,  4, NULL, NULL, NULL, NULL)," +
                        "     (1,  5, NULL, NULL, NULL, NULL)," +
                        "     (1,  6, NULL, NULL, NULL, NULL)," +
                        "     (1,  7, NULL, NULL, NULL, NULL)," +
                        "     (1,  8, NULL, NULL, NULL, NULL)," +
                        "     (1,  9, NULL, NULL, NULL, NULL)," +
                        "     (1, 10, NULL, NULL, NULL, NULL)");

        // scalar functions in DEFINE and MEASURES
        assertThat(assertions.query("SELECT M.Symbol, /* ticker symbol */ " +
                "                 M.Matchno, /* match number */ " +
                "                 M.Classy, /* classifier (label) */ " +
                "                 M.Startp, /* starting price */ " +
                "                 M.Bottomp, /* bottom price */ " +
                "                 M.Endp /* ending price */ " +
                "          FROM (VALUES " +
                "                   (1, 1, 100), " +
                "                   (1, 2, 8), " +
                "                   (1, 3, 6), " +
                "                   (1, 4, 8), " +
                "                   (1, 5, 10), " +
                "                   (1, 6, 10), " +
                "                   (1, 7, 11), " +
                "                   (1, 8, 9), " +
                "                   (1, 9, 8), " +
                "                   (1, 10, 10) " +
                "               ) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY Symbol " +
                "                   ORDER BY Tradeday " +
                "                   MEASURES " +
                "                            match_number() AS Matchno, " +
                "                            lower(classifier()) AS Classy, " +
                "                            A.Price AS Startp, " +
                "                            LAST (B.Price) AS Bottomp, " +
                "                            LAST (C.Price) AS Endp " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN (A {- B+ C+ -}) " +
                "                   SUBSET U = (A, B, C) " +
                "                   DEFINE /* A defaults to True, matches any row */ " +
                "                          B AS B.Price < PREV (B.Price) AND PREV(classifier()) = 'A' AND match_number() < 3, " +
                "                          C AS C.Price > abs(PREV (C.Price)) " +
                "                ) AS M"))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint),  CAST('a' AS varchar), CAST(8 AS integer), CAST(NULL AS integer), CAST(NULL AS integer))," +
                        "     (1, 2, 'a', 9, NULL, NULL)");

        // classifier() function with argument
        assertThat(assertions.query("SELECT M.Symbol, /* ticker symbol */ " +
                "                 M.Matchno, /* match number */ " +
                "                 M.Classy /* classifier (label) */ " +
                "          FROM (VALUES " +
                "                   (1, 1, 100), " +
                "                   (1, 2, 8), " +
                "                   (1, 3, 6), " +
                "                   (1, 4, 8), " +
                "                   (1, 5, 10), " +
                "                   (1, 6, 10), " +
                "                   (1, 7, 11), " +
                "                   (1, 8, 9), " +
                "                   (1, 9, 8), " +
                "                   (1, 10, 10) " +
                "               ) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY Symbol " +
                "                   ORDER BY Tradeday " +
                "                   MEASURES " +
                "                            match_number() AS Matchno, " +
                "                            FIRST (classifier(S), 1) AS Classy, " +
                "                            A.Price AS Startp, " +
                "                            LAST (B.Price) AS Bottomp, " +
                "                            LAST (C.Price) AS Endp " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN (A B+ C+) " +
                "                   SUBSET U = (A, B, C), " +
                "                          S = (A, C) " +
                "                   DEFINE /* A defaults to True, matches any row */ " +
                "                          B AS B.Price < PREV (B.Price) AND PREV(classifier()) = 'A' AND match_number() < 3, " +
                "                          C AS C.Price > abs(PREV (C.Price)) " +
                "                ) AS M"))
                .matches("VALUES " +
                        "     (1, CAST(1 AS bigint),  CAST(NULL AS varchar))," +
                        "     (1, 1, NULL)," +
                        "     (1, 1, 'C')," +
                        "     (1, 1, 'C')," +
                        "     (1, 2, NULL)," +
                        "     (1, 2, NULL)," +
                        "     (1, 2, 'C')");

        // FINAL semantics in MEASURES
        assertThat(assertions.query("SELECT M.Symbol, /* ticker symbol */ " +
                "                 M.Classy, /* classifier (label) */ " +
                "                 M.Bottomp /* bottom price */ " +
                "          FROM (VALUES " +
                "                   (1, 1, 10), " +
                "                   (1, 2, 8), " +
                "                   (1, 3, 6), " +
                "                   (1, 4, 8), " +
                "                   (1, 5, 10), " +
                "                   (1, 6, 9), " +
                "                   (1, 7, 8), " +
                "                   (1, 8, 10) " +
                "               ) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY Symbol " +
                "                   ORDER BY Tradeday " +
                "                   MEASURES " +
                "                            match_number() AS Matchno, " +
                "                            classifier() AS Classy, " +
                "                            A.Price AS Startp, " +
                "                            FINAL LAST (Price) AS Bottomp, " +
                "                            LAST (C.Price) AS Endp " +
                "                   ALL ROWS PER MATCH " +
                "                   AFTER MATCH SKIP PAST LAST ROW " +
                "                   PATTERN (A B+ C+) " +
                "                   SUBSET " +
                "                          U = (A, B, C) " +
                "                   DEFINE /* A defaults to True, matches any row */ " +
                "                          B AS B.Price < PREV (B.Price) AND PREV(classifier()) = 'A' AND match_number() < 3, " +
                "                          C AS C.Price > PREV (C.Price) " +
                "                ) AS M"))
                .matches("VALUES " +
                        "     (1, CAST('A' AS varchar), 10)," +
                        "     (1, 'B', 10)," +
                        "     (1, 'C', 10)," +
                        "     (1, 'C', 10)," +
                        "     (1, 'A', 10)," +
                        "     (1, 'B', 10)," +
                        "     (1, 'C', 10)");
    }
}
