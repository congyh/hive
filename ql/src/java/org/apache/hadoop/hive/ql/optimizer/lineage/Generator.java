/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.optimizer.lineage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewJoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.PTFOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UDTFOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.LevelOrderWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.optimizer.lineage.LineageCtx.Index;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class generates the lineage information for the columns
 * and tables from the plan before it goes through other
 * optimization phases.
 */
public class Generator extends Transform {

  private static final Logger LOG = LoggerFactory.getLogger(Generator.class);

  private final Set<String> hooks;
  // Note: Hive的Lineage Generator内部竟然内嵌了atlas的内容
  private static final String ATLAS_HOOK_CLASSNAME = "org.apache.atlas.hive.hook.HiveHook";

  public Generator(Set<String> hooks) {
    this.hooks = hooks;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.ql.optimizer.Transform#transform(org.apache.hadoop.hive.ql.parse.ParseContext)
   */
  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    if (hooks != null && hooks.contains(ATLAS_HOOK_CLASSNAME)) {
      // Atlas would be interested in lineage information for insert,load,create etc.
      if (!pctx.getQueryProperties().isCTAS()
          && !pctx.getQueryProperties().isMaterializedView()
          && pctx.getQueryProperties().isQuery()
          && pctx.getCreateTable() == null
          && pctx.getCreateViewDesc() == null
          && (pctx.getLoadTableWork() == null || pctx.getLoadTableWork().isEmpty())) {
        LOG.debug("Not evaluating lineage");
        return pctx;
      }
    }
    Index index = pctx.getQueryState().getLineageState().getIndex();
    if (index == null) {
      // Note: 这里是进行index初始化的地方
      // 拿到了ParseContext中的Index field的引用, 并初始化
      index = new Index();
    }

    long sTime = System.currentTimeMillis();
    // Create the lineage context
    LineageCtx lCtx = new LineageCtx(pctx, index);

    // Note: TODO: NodeProcessor是什么?
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    // Note: 定义规则集, name可以随便定义, %并不是一个wildcard.
    // TableScanOperator.getOperatorName()返回的是"TS", 意思是TableScan
    opRules.put(new RuleRegExp("R1", TableScanOperator.getOperatorName() + "%"),
      OpProcFactory.getTSProc());
    // Note: SCR
    opRules.put(new RuleRegExp("R2", ScriptOperator.getOperatorName() + "%"),
      OpProcFactory.getTransformProc());
    // Note: UDTF
    opRules.put(new RuleRegExp("R3", UDTFOperator.getOperatorName() + "%"),
      OpProcFactory.getTransformProc());
    // Note: SEL
    opRules.put(new RuleRegExp("R4", SelectOperator.getOperatorName() + "%"),
      OpProcFactory.getSelProc());
    // Note: GBY
    opRules.put(new RuleRegExp("R5", GroupByOperator.getOperatorName() + "%"),
      OpProcFactory.getGroupByProc());
    // Note: Union
    opRules.put(new RuleRegExp("R6", UnionOperator.getOperatorName() + "%"),
      OpProcFactory.getUnionProc());
    // Note: MAPJOIN
    opRules.put(new RuleRegExp("R7",
      CommonJoinOperator.getOperatorName() + "%|" + MapJoinOperator.getOperatorName() + "%"),
      OpProcFactory.getJoinProc());
    // Note: RS
    opRules.put(new RuleRegExp("R8", ReduceSinkOperator.getOperatorName() + "%"),
      OpProcFactory.getReduceSinkProc());
    // Note: LVJ
    opRules.put(new RuleRegExp("R9", LateralViewJoinOperator.getOperatorName() + "%"),
      OpProcFactory.getLateralViewJoinProc());
    // Note: PTF
    opRules.put(new RuleRegExp("R10", PTFOperator.getOperatorName() + "%"),
      OpProcFactory.getTransformProc());
    // Note: FIL
    opRules.put(new RuleRegExp("R11", FilterOperator.getOperatorName() + "%"),
      OpProcFactory.getFilterProc());

    // Note: TODO: 看到这里了
    // Note: OpProcFactory.getDefaultProc()返回的是DefaultLineage.
    // The dispatcher fires the processor corresponding to the closest matching rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(OpProcFactory.getDefaultProc(), opRules, lCtx);
    GraphWalker ogw = new LevelOrderWalker(disp, 2);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);

    LOG.debug("Time taken for lineage transform={}", (System.currentTimeMillis() - sTime));
    return pctx;
  }

}
