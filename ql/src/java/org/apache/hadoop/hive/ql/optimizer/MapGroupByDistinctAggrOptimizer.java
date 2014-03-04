/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;

/**
 * This optimization eliminates partial distinct aggragations from a map-side GroupBy
 * and its child ReduceSink operator when partial distinct aggragations are not needed.
 *
 * Note: this optimization removes distinct aggregators from a map-side GroupBy and
 *       also removes partial distinct aggregation results from the schema of the
 *       GroupBy and its child ReduceSink operator. So any optimizer which relies on
 *       such removed information, like GroupByOptimizer/ReduceSinkDeDuplication/
 *       CorrelationOptimizer, should appear before this optimizer.
 */
public class MapGroupByDistinctAggrOptimizer implements Transform {

  public MapGroupByDistinctAggrOptimizer() {
  }

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    // process group-by pattern
    opRules.put(new RuleRegExp("R1",
        GroupByOperator.getOperatorName() + "%" +
            ReduceSinkOperator.getOperatorName() + "%" +
            GroupByOperator.getOperatorName() + "%"),
        getMapGroupbyProc());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp =
        new DefaultRuleDispatcher(getDefaultProc(), opRules,
            new MapGroupByOptimizerContext(pctx.getOpParseCtx()));
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);

    return pctx;
  }

  private NodeProcessor getDefaultProc() {
    return new NodeProcessor() {
      @Override
      public Object process(Node nd, Stack<Node> stack,
          NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
        return null;
      }
    };
  }

  private NodeProcessor getMapGroupbyProc() {
    return new MapGroupByProcessor();
  }

  /**
   * MapGroupByProcessor.
   *
   */
  public class MapGroupByProcessor implements NodeProcessor {

    public MapGroupByProcessor() {
    }

    // Check if the group by operator has already been processed
    private boolean checkGroupByOperatorProcessed(
        MapGroupByOptimizerContext groupByOptimizerContext,
        GroupByOperator groupByOp) {

      // The group by operator has already been processed
      if (groupByOptimizerContext.getListGroupByOperatorsProcessed().contains(groupByOp)) {
        return true;
      }

      groupByOptimizerContext.getListGroupByOperatorsProcessed().add(groupByOp);
      return false;
    }

    private void processGroupBy(MapGroupByOptimizerContext ctx, GroupByOperator groupByOp)
        throws SemanticException {
      GroupByDesc groupByOpDesc = groupByOp.getConf();
      ReduceSinkOperator reduceSinkOp = (ReduceSinkOperator)groupByOp.getChildOperators().get(0);
      GroupByDesc childGroupByDesc =
        ((GroupByOperator) (reduceSinkOp.getChildOperators().get(0))).getConf();

      // Eliminate un-necessary partial distinct aggregations for map-side distinct aggregations
      if (groupByOpDesc.getMode() == GroupByDesc.Mode.HASH &&
          !groupByOpDesc.getGroupKeyNotReductionKey() &&
          groupByOpDesc.isDistinct() && childGroupByDesc.isDistinct()) {
        RowResolver groupByRowResolver = ctx.getOpToParseCtxMap().get(groupByOp).getRowResolver();
        ArrayList<ColumnInfo> groupBySignature = groupByRowResolver.getRowSchema().getSignature();
        ArrayList<String> outputColumnNames = groupByOpDesc.getOutputColumnNames();
        ArrayList<AggregationDesc> aggregators = groupByOpDesc.getAggregators();
        ArrayList<AggregationDesc> newAggregators = new ArrayList<AggregationDesc>();
        ArrayList<String> newOutputColumnNames = new ArrayList<String>();

        RowResolver reduceSinkRowResolver = ctx.getOpToParseCtxMap().get(reduceSinkOp).getRowResolver();
        ArrayList<ColumnInfo> reduceSinkSignature = reduceSinkRowResolver.getRowSchema().getSignature();
        Map<String, ExprNodeDesc> redudeSinkColExprMap = reduceSinkOp.getColumnExprMap();
        ReduceSinkDesc reduceSinkDesc = reduceSinkOp.getConf();
        ArrayList<ExprNodeDesc> valueCols = reduceSinkDesc.getValueCols();
        ArrayList<String> outputValueColumnNames = reduceSinkDesc.getOutputValueColumnNames();
        ArrayList<ExprNodeDesc> newValueCols = new ArrayList<ExprNodeDesc>();
        ArrayList<String> newOutputValueColumnNames = new ArrayList<String>();

        int aggrColStartingIndex = groupByOpDesc.getKeys().size();
        // Retain key columns for the GroupBy op
        for (int i = 0; i < aggrColStartingIndex; i++) {
          newOutputColumnNames.add(outputColumnNames.get(i));
        }
  
        // For each aggregation
        for (int i = 0; i < aggregators.size(); i++) {
          AggregationDesc aggregator = aggregators.get(i);
          if (aggregator.getDistinct()) {
            // Remove the output column for the distinct aggregation from the GroupBy op
            String outputCol = outputColumnNames.get(aggrColStartingIndex + i);
            String[] tabColAlias = groupByRowResolver.reverseLookup(outputCol);
            ColumnInfo colInfo = 
              groupByRowResolver.getFieldMap(tabColAlias[0]).remove(tabColAlias[1]);
            groupByRowResolver.getInvRslvMap().remove(colInfo.getInternalName());
            groupBySignature.remove(colInfo);

            // Remove the output column for the distinct aggregation from the ReduceSink op
            outputCol = outputValueColumnNames.get(i);
            outputCol = Utilities.ReduceField.VALUE.toString() + "." + outputCol;
            tabColAlias = reduceSinkRowResolver.reverseLookup(outputCol);
            colInfo = reduceSinkRowResolver.getFieldMap(tabColAlias[0]).remove(tabColAlias[1]);
            reduceSinkRowResolver.getInvRslvMap().remove(colInfo.getInternalName());
            redudeSinkColExprMap.remove(outputCol);
            reduceSinkSignature.remove(colInfo);
          } else {
            newAggregators.add(aggregator);
            newOutputColumnNames.add(outputColumnNames.get(aggrColStartingIndex + i));
            
            newValueCols.add(valueCols.get(i));
            newOutputValueColumnNames.add(outputValueColumnNames.get(i));
          }
        }
        
        // Update GroupByOpDesc for the GroupBy op
        groupByOpDesc.setAggregators(newAggregators);
        groupByOpDesc.setOutputColumnNames(newOutputColumnNames);

        // Update ReduceSinkDesc for the ReduceSink operator
        reduceSinkDesc.setValueCols(newValueCols);
        reduceSinkDesc.setOutputValueColumnNames(newOutputValueColumnNames);
        TableDesc newValueTable = PlanUtils.getReduceValueTableDesc(PlanUtils
          .getFieldSchemasFromColumnList(newValueCols, newOutputValueColumnNames, 0, ""));
        reduceSinkDesc.setValueSerializeInfo(newValueTable);
      }
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      // GBY,RS,GBY... (top to bottom)
      GroupByOperator groupByOp = (GroupByOperator) stack.get(stack.size() - 3);

      MapGroupByOptimizerContext ctx = (MapGroupByOptimizerContext) procCtx;

      if (!checkGroupByOperatorProcessed(ctx, groupByOp)) {
        processGroupBy(ctx, groupByOp);
      }
      return null;
    }
  }

  public class MapGroupByOptimizerContext implements NodeProcessorCtx {
    List<GroupByOperator> listGroupByOperatorsProcessed;
    private HashMap<Operator<? extends OperatorDesc>, OpParseContext> opToParseCtxMap;

    public MapGroupByOptimizerContext(
      HashMap<Operator<? extends OperatorDesc>, OpParseContext> opToParseCtxMap) {
      this.opToParseCtxMap = opToParseCtxMap;
      listGroupByOperatorsProcessed = new ArrayList<GroupByOperator>();
    }

    public List<GroupByOperator> getListGroupByOperatorsProcessed() {
      return listGroupByOperatorsProcessed;
    }

    public HashMap<Operator<? extends OperatorDesc>, OpParseContext> getOpToParseCtxMap() {
      return opToParseCtxMap;
    }
  }
}
