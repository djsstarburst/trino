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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.metadata.TableHandle;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.optimizations.joins.CommonSubplanExtractor;
import io.trino.sql.planner.optimizations.joins.CommonSubplanExtractor.ExtractedSubplan;
import io.trino.sql.planner.optimizations.joins.PlanSignatureEvaluator;
import io.trino.sql.planner.optimizations.joins.PlanSignatureEvaluator.NodeSignature;
import io.trino.sql.planner.optimizations.joins.TableSignature;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.MaterializedResult;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestPlanSignatureEvaluator
        extends BasePlanTest
{
    private static final Logger log = Logger.get(TestPlanSignatureEvaluator.class);

    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();
        LocalQueryRunner queryRunner = LocalQueryRunner.builder(session)
                .build();
        queryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());
        return queryRunner;
    }

    @Test
    public void testQ21Signatures()
    {
        LocalQueryRunner queryRunner = createLocalQueryRunner();
        // Means: Count the lineitems that are late from a supplier but all other suppliers on
        // the same order are on time.
        @Language("SQL") String sql = "select count(*) as numwait" +
                " from lineitem l1" +
                " where" +
                "     l1.receiptdate > l1.commitdate" +
                // Means: There is at least one other supplier for a part in the same order
                "     and exists (select * from lineitem l2" +
                "         where" +
                "             l2.orderkey = l1.orderkey" +
                "             and l2.suppkey <> l1.suppkey)" +
                // Means: None of the other supplers on the order is late.
                "     and not exists (select * from lineitem l3" +
                "         where" +
                "             l3.orderkey = l1.orderkey" +
                "             and l3.suppkey <> l1.suppkey" +
                "             and l3.receiptdate > l3.commitdate)";

        List<PlanNode> matchingNodes = testSql(queryRunner, sql, node -> node instanceof TableScanNode);
        log.info("matchingNodes: %s", matchingNodes);
    }

    @Test
    public void testAggregateSignatures()
    {
        LocalQueryRunner queryRunner = createLocalQueryRunner();
        @Language("SQL") String sql = "" +
                " select count(*) from nation where name like 'fred%'" +
                " union all" +
                " select count(*) from nation where name like 'joan%'";
        List<PlanNode> matchingNodes = testSql(queryRunner, sql, node -> node instanceof TableScanNode);
        log.info("matchingNodes: %s", matchingNodes);
    }

    @Test
    public void testSimpleAggregations()
    {
        LocalQueryRunner queryRunner = createLocalQueryRunner();
        @Language("SQL") String sql = "select ROW((select min(orderkey) FROM lineitem), (SELECT max(orderkey) FILTER (WHERE suppkey > 3) from lineitem))";
        testSql(queryRunner, sql, node -> node instanceof TableScanNode);
    }

    private List<PlanNode> testSql(LocalQueryRunner queryRunner, String sql, Predicate<PlanNode> predicate)
    {
        Plan plan = plan(sql, OPTIMIZED);
        MaterializedResult result = queryRunner.execute("EXPLAIN " + sql);
        log.info("EXPLAIN %s: %s", sql, result.getOnlyValue());
        PlanSignatureEvaluator evaluator = new PlanSignatureEvaluator(ImmutableList.of(plan.getRoot()));
        Map<TableSignature, List<NodeSignature>> aggNodeSignatures = evaluator.getSignaturesPlanNodeIdsForDescription(Optional.of("agg"));
        // For now...
        checkArgument(aggNodeSignatures.size() == 1);
        aggNodeSignatures.forEach((tableSig, nodeSigs) -> {
            TableHandle handle = tableSig.getTableHandles().iterator().next();
            List<ExtractedSubplan> extractedSubplans = new CommonSubplanExtractor()
                    .getSubplansForSignature(handle, nodeSigs);
            log.info("Extracted plans: %s", extractedSubplans);
        });
        evaluator.getTableSignature(plan.getRoot().getId());
        return findApplicableNodes(ImmutableList.of(plan.getRoot()), predicate);
    }

    private List<PlanNode> findApplicableNodes(List<PlanNode> roots, Predicate<PlanNode> predicate)
    {
        List<PlanNode> nodes = new ArrayList<>();
        for (PlanNode node : roots) {
            findApplicableNodes(node, nodes, predicate);
        }
        return nodes;
    }

    private void findApplicableNodes(PlanNode node, List<PlanNode> nodes, Predicate<PlanNode> predicate)
    {
        if (predicate.test(node)) {
            nodes.add(node);
        }
        else {
            node.getSources().forEach(source -> findApplicableNodes(source, nodes, predicate));
        }
    }
}
