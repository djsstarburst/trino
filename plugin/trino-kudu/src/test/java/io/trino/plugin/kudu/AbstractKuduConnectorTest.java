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
package io.trino.plugin.kudu;

import io.trino.sql.planner.plan.LimitNode;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static io.trino.plugin.kudu.KuduQueryRunnerFactory.createKuduQueryRunnerTpch;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertTrue;

public abstract class AbstractKuduConnectorTest
        extends BaseConnectorTest
{
    private TestingKuduServer kuduServer;

    protected abstract String getKuduServerVersion();

    protected abstract Optional<String> getKuduSchemaEmulationPrefix();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        kuduServer = new TestingKuduServer(getKuduServerVersion());
        return createKuduQueryRunnerTpch(kuduServer, getKuduSchemaEmulationPrefix(), REQUIRED_TPCH_TABLES);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (kuduServer != null) {
            kuduServer.close();
            kuduServer = null;
        }
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_DELETE:
                return true;
            case SUPPORTS_RENAME_SCHEMA:
            case SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT:
            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
            case SUPPORTS_ARRAY:
            case SUPPORTS_NOT_NULL_CONSTRAINT:
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_ROW_TYPE:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        String extra = "nullable, encoding=auto, compression=default";
        MaterializedResult expectedColumns = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", extra, "")
                .row("custkey", "bigint", extra, "")
                .row("orderstatus", "varchar", extra, "")
                .row("totalprice", "double", extra, "")
                .row("orderdate", "varchar", extra, "")
                .row("orderpriority", "varchar", extra, "")
                .row("clerk", "varchar", extra, "")
                .row("shippriority", "integer", extra, "")
                .row("comment", "varchar", extra, "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertEquals(actualColumns, expectedColumns);
    }

    @Test
    @Override
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "nullable, encoding=auto, compression=default", "")
                .row("custkey", "bigint", "nullable, encoding=auto, compression=default", "")
                .row("orderstatus", "varchar", "nullable, encoding=auto, compression=default", "")
                .row("totalprice", "double", "nullable, encoding=auto, compression=default", "")
                .row("orderdate", "varchar", "nullable, encoding=auto, compression=default", "")
                .row("orderpriority", "varchar", "nullable, encoding=auto, compression=default", "")
                .row("clerk", "varchar", "nullable, encoding=auto, compression=default", "")
                .row("shippriority", "integer", "nullable, encoding=auto, compression=default", "")
                .row("comment", "varchar", "nullable, encoding=auto, compression=default", "")
                .build();

        assertEquals(actual, expectedParametrizedVarchar);
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .matches("CREATE TABLE kudu\\.\\w+\\.orders \\Q(\n" +
                        "   orderkey bigint COMMENT '' WITH ( nullable = true ),\n" +
                        "   custkey bigint COMMENT '' WITH ( nullable = true ),\n" +
                        "   orderstatus varchar COMMENT '' WITH ( nullable = true ),\n" +
                        "   totalprice double COMMENT '' WITH ( nullable = true ),\n" +
                        "   orderdate varchar COMMENT '' WITH ( nullable = true ),\n" +
                        "   orderpriority varchar COMMENT '' WITH ( nullable = true ),\n" +
                        "   clerk varchar COMMENT '' WITH ( nullable = true ),\n" +
                        "   shippriority integer COMMENT '' WITH ( nullable = true ),\n" +
                        "   comment varchar COMMENT '' WITH ( nullable = true )\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   number_of_replicas = 3,\n" +
                        "   partition_by_hash_buckets = 2,\n" +
                        "   partition_by_hash_columns = ARRAY['row_uuid'],\n" +
                        "   partition_by_range_columns = ARRAY['row_uuid'],\n" +
                        "   range_partitions = '[{\"lower\":null,\"upper\":null}]'\n" +
                        ")");

        assertUpdate("CREATE TABLE IF NOT EXISTS test_show_create_table (\n" +
                "id INT WITH (primary_key=true),\n" +
                "user_name VARCHAR\n" +
                ") WITH (\n" +
                " partition_by_hash_columns = ARRAY['id'],\n" +
                " partition_by_hash_buckets = 2," +
                " number_of_replicas = 1\n" +
                ")");

        MaterializedResult result = computeActual("SHOW CREATE TABLE test_show_create_table");
        String sqlStatement = (String) result.getOnlyValue();
        String tableProperties = sqlStatement.split("\\)\\s*WITH\\s*\\(")[1];
        assertTableProperty(tableProperties, "number_of_replicas", "1");
        assertTableProperty(tableProperties, "partition_by_hash_columns", Pattern.quote("ARRAY['id']"));
        assertTableProperty(tableProperties, "partition_by_hash_buckets", "2");

        assertUpdate("DROP TABLE test_show_create_table");
    }

    @Test
    public void testRowDelete()
    {
        assertUpdate("CREATE TABLE IF NOT EXISTS test_row_delete (" +
                "id INT WITH (primary_key=true), " +
                "second_id INT, " +
                "user_name VARCHAR" +
                ") WITH (" +
                " partition_by_hash_columns = ARRAY['id'], " +
                " partition_by_hash_buckets = 2" +
                ")");

        assertUpdate("INSERT INTO test_row_delete VALUES (0, 1, 'user0'), (3, 4, 'user2'), (2, 3, 'user2'), (1, 2, 'user1')", 4);
        assertQuery("SELECT count(*) FROM test_row_delete", "VALUES 4");

        assertUpdate("DELETE FROM test_row_delete WHERE second_id = 4", 1);
        assertQuery("SELECT count(*) FROM test_row_delete", "VALUES 3");

        assertUpdate("DELETE FROM test_row_delete WHERE user_name = 'user1'", 1);
        assertQuery("SELECT count(*) FROM test_row_delete", "VALUES 2");

        assertUpdate("DELETE FROM test_row_delete WHERE id = 0", 1);
        assertQuery("SELECT * FROM test_row_delete", "VALUES (2, 3, 'user2')");

        assertUpdate("DROP TABLE test_row_delete");
    }

    @Override
    protected void testColumnName(String columnName, boolean delimited)
    {
        String nameInSql = columnName;
        if (delimited) {
            nameInSql = "\"" + columnName.replace("\"", "\"\"") + "\"";
        }
        String tableName = "tcn_" + nameInSql.toLowerCase(ENGLISH).replaceAll("[^a-z0-9]", "") + randomTableSuffix();

        try {
            // TODO test with both CTAS *and* CREATE TABLE + INSERT, since they use different connector API methods.
            assertUpdate("" +
                    "CREATE TABLE " + tableName + "(key varchar(50) WITH (primary_key=true), " + nameInSql + " varchar(50) WITH (nullable=true)) " +
                    "WITH (partition_by_hash_columns = ARRAY['key'], partition_by_hash_buckets = 3)");
        }
        catch (RuntimeException e) {
            if (isColumnNameRejected(e, columnName, delimited)) {
                // It is OK if give column name is not allowed and is clearly rejected by the connector.
                return;
            }
            throw e;
        }
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES ('null value', NULL), ('sample value', 'abc'), ('other value', 'xyz')", 3);

            // SELECT *
            assertQuery("SELECT * FROM " + tableName, "VALUES ('null value', NULL), ('sample value', 'abc'), ('other value', 'xyz')");

            // projection
            assertQuery("SELECT " + nameInSql + " FROM " + tableName, "VALUES (NULL), ('abc'), ('xyz')");

            // predicate
            assertQuery("SELECT key FROM " + tableName + " WHERE " + nameInSql + " IS NULL", "VALUES ('null value')");
            assertQuery("SELECT key FROM " + tableName + " WHERE " + nameInSql + " = 'abc'", "VALUES ('sample value')");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testProjection()
    {
        assertUpdate("CREATE TABLE IF NOT EXISTS test_projection (" +
                "id INT WITH (primary_key=true), " +
                "user_name VARCHAR " +
                ") WITH (" +
                " partition_by_hash_columns = ARRAY['id'], " +
                " partition_by_hash_buckets = 2" +
                ")");

        assertUpdate("INSERT INTO test_projection VALUES (0, 'user0'), (2, 'user2'), (1, 'user1')", 3);

        assertQuery("SELECT id, 'test' FROM test_projection ORDER BY id", "VALUES (0, 'test'), (1, 'test'), (2, 'test')");

        assertUpdate("DROP TABLE test_projection");
    }

    @Test
    @Override
    public void testExplainAnalyzeWithDeleteWithSubquery()
    {
        String tableName = "test_delete_" + randomTableSuffix();

        // delete using a subquery
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM nation", 25);
        assertExplainAnalyze("EXPLAIN ANALYZE DELETE FROM " + tableName + " WHERE regionkey IN (SELECT regionkey FROM region WHERE name LIKE 'A%' LIMIT 1)",
                "SemiJoin.*");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override
    public void testCreateTable()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
        throw new SkipException("TODO");
    }

    @Override
    public void testCreateTableWithColumnComment()
    {
        // TODO https://github.com/trinodb/trino/issues/12469 Support column comment when creating tables
        String tableName = "test_create_" + randomTableSuffix();

        assertQueryFails(
                "CREATE TABLE " + tableName + "(" +
                        "id INT WITH (primary_key=true)," +
                        "a VARCHAR COMMENT 'test comment')" +
                        "WITH (partition_by_hash_columns = ARRAY['id'], partition_by_hash_buckets = 2)",
                "This connector does not support creating tables with column comment");

        assertUpdate("DROP TABLE IF EXISTS " + tableName);
    }

    @Override
    public void testDropTable()
    {
        assertThatThrownBy(super::testDropTable)
                .hasMessage("Table partitioning must be specified using setRangePartitionColumns or addHashPartitions");
        throw new SkipException("TODO Enable the test once Kudu connector can create tables with default partitions");
    }

    @Override
    protected String tableDefinitionForAddColumn()
    {
        return "(x VARCHAR WITH (primary_key=true)) WITH (partition_by_hash_columns = ARRAY['x'], partition_by_hash_buckets = 2)";
    }

    @Test
    @Override
    public void testAddColumnWithComment()
    {
        String tableName = "test_add_column_with_comment" + randomTableSuffix();

        assertUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                "id INT WITH (primary_key=true), " +
                "a_varchar VARCHAR" +
                ") WITH (" +
                " partition_by_hash_columns = ARRAY['id'], " +
                " partition_by_hash_buckets = 2" +
                ")");

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN b_varchar varchar COMMENT 'test new column comment'");
        assertThat(getColumnComment(tableName, "b_varchar")).isEqualTo("test new column comment");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override
    public void testInsert()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
        throw new SkipException("TODO");
    }

    @Test
    @Override
    public void testInsertUnicode()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
        throw new SkipException("TODO");
    }

    @Test
    @Override
    public void testInsertHighestUnicodeCharacter()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
        throw new SkipException("TODO");
    }

    @Override
    public void testInsertNegativeDate()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
        throw new SkipException("TODO");
    }

    @Override
    public void testInsertRowConcurrently()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
        throw new SkipException("TODO");
    }

    @Override
    public void testAddColumnConcurrently()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
        throw new SkipException("TODO");
    }

    @Test
    @Override
    public void testDelete()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
        throw new SkipException("TODO");
    }

    @Test
    @Override
    public void testDeleteWithSemiJoin()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
        throw new SkipException("TODO");
    }

    @Test
    @Override
    public void testDeleteWithVarcharPredicate()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
        throw new SkipException("TODO");
    }

    @Test
    @Override
    public void testDeleteWithComplexPredicate()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
        throw new SkipException("TODO");
    }

    @Test
    @Override
    public void testDeleteWithSubquery()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
        throw new SkipException("TODO");
    }

    @Test
    @Override
    public void testWrittenStats()
    {
        // TODO Kudu connector supports CTAS and inserts, but the test would fail
        throw new SkipException("TODO");
    }

    @Override
    public void testReadMetadataWithRelationsConcurrentModifications()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
        throw new SkipException("TODO");
    }

    @Test
    @Override
    public void testCreateTableAsSelectNegativeDate()
    {
        // Map date column type to varchar
        String tableName = "negative_date_" + randomTableSuffix();

        try {
            assertUpdate(format("CREATE TABLE %s AS SELECT DATE '-0001-01-01' AS dt", tableName), 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES '-0001-01-01'");
            assertQuery(format("SELECT * FROM %s WHERE dt = '-0001-01-01'", tableName), "VALUES '-0001-01-01'");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    @Override
    public void testDateYearOfEraPredicate()
    {
        assertThatThrownBy(super::testDateYearOfEraPredicate)
                .hasStackTraceContaining("Cannot apply operator: varchar = date");
    }

    @Test
    @Override
    public void testCharVarcharComparison()
    {
        assertThatThrownBy(super::testCharVarcharComparison)
                .hasMessageContaining("For query: ")
                .hasMessageContaining("Actual rows")
                .hasMessageContaining("Expected rows");

        throw new SkipException("TODO");
    }

    @Test
    public void testLimitPushdown()
    {
        assertThat(query("SELECT name FROM nation LIMIT 30")).isNotFullyPushedDown(LimitNode.class); // Use high limit for result determinism
    }

    @Test
    public void testMergeSimpleSelect()
    {
        String targetTable = "simple_select_target";
        String sourceTable = "simple_select_source";
        try {
            assertUpdate(format("CREATE TABLE %s (customer VARCHAR WITH (primary_key=true), purchases INT, address VARCHAR)" +
                    "WITH (" +
                    " partition_by_hash_columns = ARRAY['customer'], " +
                    " partition_by_hash_buckets = 2" +
                    ")", targetTable));

            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

            assertUpdate(format("CREATE TABLE %s (customer VARCHAR WITH (primary_key=true), purchases INT, address VARCHAR)" +
                    "WITH (" +
                    " partition_by_hash_columns = ARRAY['customer'], " +
                    " partition_by_hash_buckets = 2" +
                    ")", sourceTable));

            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable), 4);

            assertUpdate(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                            "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                            "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                            "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)",
                    4);

            assertQuery("SELECT * FROM " + targetTable, "SELECT * FROM (VALUES('Aaron', 11, 'Arches'), ('Ed', 7, 'Etherville'), ('Bill', 7, 'Buena'), ('Dave', 22, 'Darbyshire'))");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + targetTable);
            assertUpdate("DROP TABLE IF EXISTS " + sourceTable);
        }
    }

    @Test
    public void testMergeMultipleOperations()
    {
        String targetTable = "merge_multiple";
        try {
            int targetCustomerCount = 10;
            String originalInsertFirstHalf = IntStream.range(0, targetCustomerCount / 2)
                    .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 1000, 91000, intValue, intValue))
                    .collect(joining(", "));
            String originalInsertSecondHalf = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                    .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 2000, 92000, intValue, intValue))
                    .collect(joining(", "));

            assertUpdate(format("CREATE TABLE %s (customer VARCHAR WITH (primary_key=true), purchases INT, zipcode INT, spouse VARCHAR, address VARCHAR) " +
                    "WITH (" +
                    " partition_by_hash_columns = ARRAY['customer'], " +
                    " partition_by_hash_buckets = 2" +
                    ")", targetTable));

            assertUpdate(format("INSERT INTO %s (customer, purchases, zipcode, spouse, address) VALUES %s, %s", targetTable, originalInsertFirstHalf, originalInsertSecondHalf), targetCustomerCount);

            String firstMergeSource = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                    .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jill_%s', '%s Eop Ct')", intValue, 3000, 83000, intValue, intValue))
                    .collect(joining(", "));

            assertUpdate(format("MERGE INTO %s t USING (SELECT * FROM (VALUES %s)) AS s(customer, purchases, zipcode, spouse, address)", targetTable, firstMergeSource) +
                            "    ON t.customer = s.customer" +
                            "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases, zipcode = s.zipcode, spouse = s.spouse, address = s.address",
                    targetCustomerCount / 2);

            assertQuery(
                    "SELECT customer, purchases, zipcode, spouse, address FROM " + targetTable,
                    format("SELECT * FROM (VALUES %s, %s) AS v(customer, purchases, zipcode, spouse, address)", originalInsertFirstHalf, firstMergeSource));

            String nextInsert = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                    .mapToObj(intValue -> format("('jack_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 4000, 74000, intValue, intValue))
                    .collect(joining(", "));
            assertUpdate(format("INSERT INTO %s (customer, purchases, zipcode, spouse, address) VALUES %s", targetTable, nextInsert), targetCustomerCount / 2);

            String secondMergeSource = IntStream.range(0, targetCustomerCount * 3 / 2)
                    .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jen_%s', '%s Poe Ct')", intValue, 5000, 85000, intValue, intValue))
                    .collect(joining(", "));

            assertUpdate(format("MERGE INTO %s t USING (SELECT * FROM (VALUES %s)) AS s(customer, purchases, zipcode, spouse, address)", targetTable, secondMergeSource) +
                            "    ON t.customer = s.customer" +
                            "    WHEN MATCHED AND t.zipcode = 91000 THEN DELETE" +
                            "    WHEN MATCHED AND s.zipcode = 85000 THEN UPDATE SET zipcode = 60000" +
                            "    WHEN MATCHED THEN UPDATE SET zipcode = s.zipcode, spouse = s.spouse, address = s.address" +
                            "    WHEN NOT MATCHED THEN INSERT (customer, purchases, zipcode, spouse, address) VALUES(s.customer, s.purchases, s.zipcode, s.spouse, s.address)",
                    targetCustomerCount * 3 / 2);

            String updatedBeginning = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                    .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jill_%s', '%s Eop Ct')", intValue, 3000, 60000, intValue, intValue))
                    .collect(joining(", "));
            String updatedMiddle = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                    .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jen_%s', '%s Poe Ct')", intValue, 5000, 85000, intValue, intValue))
                    .collect(joining(", "));
            String updatedEnd = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                    .mapToObj(intValue -> format("('jack_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 4000, 74000, intValue, intValue))
                    .collect(joining(", "));

            assertQuery(
                    "SELECT customer, purchases, zipcode, spouse, address FROM " + targetTable,
                    format("SELECT * FROM (VALUES %s, %s, %s) AS v(customer, purchases, zipcode, spouse, address)", updatedBeginning, updatedMiddle, updatedEnd));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + targetTable);
        }
    }

    @Test
    public void testMergeInsertAll()
    {
        String targetTable = "merge_update_all";
        try {
            assertUpdate(format("CREATE TABLE %s (customer VARCHAR WITH (primary_key=true), purchases INT, address VARCHAR)" +
                    "WITH (" +
                    " partition_by_hash_columns = ARRAY['customer'], " +
                    " partition_by_hash_buckets = 3" +
                    ")", targetTable));

            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 11, 'Antioch'), ('Bill', 7, 'Buena')", targetTable), 2);

            assertUpdate(format("MERGE INTO %s t USING ", targetTable) +
                            "(SELECT * FROM (VALUES ('Carol', 9, 'Centreville'), ('Dave', 22, 'Darbyshire'))) AS s(customer, purchases, address)" +
                            "ON (t.customer = s.customer)" +
                            "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)",
                    2);

            assertQuery(
                    "SELECT * FROM " + targetTable,
                    "SELECT * FROM (VALUES('Aaron', 11, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 9, 'Centreville'), ('Dave', 22, 'Darbyshire'))");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + targetTable);
        }
    }

    @Test
    public void testMergeAllColumnsUpdated()
    {
        String targetTable = "merge_all_columns_updated_target";
        String sourceTable = "merge_all_columns_updated_source";
        try {
            assertUpdate(format("CREATE TABLE %s (customer VARCHAR WITH (primary_key=true), purchases INT, address VARCHAR) " +
                    "WITH (" +
                    " partition_by_hash_columns = ARRAY['customer'], " +
                    " partition_by_hash_buckets = 2" +
                    ")", targetTable));
            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Dave', 11, 'Devon'), ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge')", targetTable), 4);

            assertUpdate(format("CREATE TABLE %s (customer VARCHAR WITH (primary_key=true), purchases INT, address VARCHAR) " +
                    "WITH (" +
                    " partition_by_hash_columns = ARRAY['customer'], " +
                    " partition_by_hash_buckets = 2" +
                    ")", sourceTable));

            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Dave', 11, 'Darbyshire'), ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Ed', 7, 'Etherville')", sourceTable), 4);

            assertUpdate(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                            "    WHEN MATCHED THEN UPDATE SET customer = CONCAT(t.customer, '_updated'), purchases = s.purchases + t.purchases, address = s.address",
                    3);

            assertQuery(
                    "SELECT * FROM " + targetTable,
                    "SELECT * FROM (VALUES ('Dave_updated', 22, 'Darbyshire'), ('Aaron_updated', 11, 'Arches'), ('Bill', 7, 'Buena'), ('Carol_updated', 12, 'Centreville'))");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + targetTable);
            assertUpdate("DROP TABLE IF EXISTS " + sourceTable);
        }
    }

    @Test
    public void testMergeAllMatchesDeleted()
    {
        String targetTable = "merge_all_matches_deleted_target";
        String sourceTable = "merge_all_matches_deleted_source";
        try {
            assertUpdate(format("CREATE TABLE %s (customer VARCHAR WITH (primary_key=true), purchases INT, address VARCHAR) " +
                    "WITH (" +
                    " partition_by_hash_columns = ARRAY['customer'], " +
                    " partition_by_hash_buckets = 2" +
                    ")", targetTable));

            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

            assertUpdate(format("CREATE TABLE %s (customer VARCHAR WITH (primary_key=true), purchases INT, address VARCHAR) " +
                    "WITH (" +
                    " partition_by_hash_columns = ARRAY['customer'], " +
                    " partition_by_hash_buckets = 2" +
                    ")", sourceTable));

            assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire'), ('Ed', 7, 'Etherville')", sourceTable), 4);

            assertUpdate(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                            "    WHEN MATCHED THEN DELETE",
                    3);

            assertQuery("SELECT * FROM " + targetTable, "SELECT * FROM (VALUES ('Bill', 7, 'Buena'))");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + targetTable);
            assertUpdate("DROP TABLE IF EXISTS " + sourceTable);
        }
    }

    @Test
    public void testMergeLimes()
    {
        String targetTable = "merge_with_various_formats";
        String sourceTable = "merge_simple_source";
        try {
            assertUpdate(format("CREATE TABLE %s (customer VARCHAR WITH (primary_key=true), purchase VARCHAR) " +
                    "WITH (" +
                    " partition_by_hash_columns = ARRAY['customer'], " +
                    " partition_by_hash_buckets = 5" +
                    ")", targetTable));

            assertUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Dave', 'dates'), ('Lou', 'limes'), ('Carol', 'candles')", targetTable), 3);
            assertQuery("SELECT * FROM " + targetTable, "SELECT * FROM (VALUES ('Dave', 'dates'), ('Lou', 'limes'), ('Carol', 'candles'))");

            assertUpdate(format("CREATE TABLE %s (customer VARCHAR WITH (primary_key=true), purchase VARCHAR) " +
                    "WITH (" +
                    " partition_by_hash_columns = ARRAY['customer'], " +
                    " partition_by_hash_buckets = 5" +
                    ")", sourceTable));

            assertUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Craig', 'candles'), ('Len', 'limes'), ('Joe', 'jellybeans')", sourceTable), 3);

            String sql = format("MERGE INTO %s t USING %s s ON (t.purchase = s.purchase)", targetTable, sourceTable) +
                    "    WHEN MATCHED AND s.purchase = 'limes' THEN DELETE" +
                    "    WHEN MATCHED THEN UPDATE SET customer = CONCAT(t.customer, '_', s.customer)" +
                    "    WHEN NOT MATCHED THEN INSERT (customer, purchase) VALUES(s.customer, s.purchase)";

            assertUpdate(sql, 3);

            assertQuery("SELECT * FROM " + targetTable, "SELECT * FROM (VALUES ('Dave', 'dates'), row('Carol_Craig', 'candles'), row('Joe', 'jellybeans'))");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + targetTable);
            assertUpdate("DROP TABLE IF EXISTS " + sourceTable);
        }
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("time")
                || typeName.equals("time(6)")
                || typeName.equals("timestamp(6)")
                || typeName.equals("timestamp(3) with time zone")
                || typeName.equals("timestamp(6) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        if (typeName.equals("date") // date gets stored as varchar
                || typeName.equals("varbinary") // TODO (https://github.com/trinodb/trino/issues/3416)
                || (typeName.startsWith("char") && dataMappingTestSetup.getSampleValueLiteral().contains(" "))) { // TODO: https://github.com/trinodb/trino/issues/3597
            // TODO this should either work or fail cleanly
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("Kudu connector does not support column default values");
    }

    @Override
    protected String tableDefinitionForQueryLoggingCount()
    {
        return "( " +
                " foo_1 int WITH (primary_key=true), " +
                " foo_2_4 int " +
                ") " +
                "WITH ( " +
                " partition_by_hash_columns = ARRAY['foo_1'], " +
                " partition_by_hash_buckets = 2 " +
                ")";
    }

    private void assertTableProperty(String tableProperties, String key, String regexValue)
    {
        assertTrue(Pattern.compile(key + "\\s*=\\s*" + regexValue + ",?\\s+").matcher(tableProperties).find(),
                "Not found: " + key + " = " + regexValue + " in " + tableProperties);
    }
}
