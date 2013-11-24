/*
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.oryx.kmeans.common;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import com.cloudera.oryx.common.OryxTest;

/**
 * Tests implementations of {@link KMeansEvalStrategy}.
 */
public final class KMeansEvalStrategyTest extends OryxTest {

  private List<ClusterValidityStatistics> stats;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    // k,replica,testCost,trainCost,varInfo,vanDongen
    stats = ImmutableList.of(
        ClusterValidityStatistics.parse("1,0,10,20,0.5,0.2"),
        ClusterValidityStatistics.parse("2,0,5,17,0.4,0.4"),
        ClusterValidityStatistics.parse("2,1,7,21,0.2,0.5"),
        ClusterValidityStatistics.parse("2,2,8,13,0.3,0.6"),
        ClusterValidityStatistics.parse("5,0,3,11,0.5,0.1"),
        ClusterValidityStatistics.parse("5,1,5,10,0.4,0.2"),
        ClusterValidityStatistics.parse("5,2,6,12,0.3,0.3"));
  }

  @Test
  public void testFixedK() throws Exception {
    FixedKEvalStrategy fk = new FixedKEvalStrategy(2);
    assertEquals(stats.subList(3, 4), fk.evaluate(stats));
    fk = new FixedKEvalStrategy(5);
    assertEquals(stats.subList(4, 5), fk.evaluate(stats));
  }

  @Test
  public void testLowCostStable() throws Exception {
    LowCostStableEvalStrategy low = new LowCostStableEvalStrategy(0.4, true);
    assertEquals(stats.subList(6, 7), low.evaluate(stats));
    low = new LowCostStableEvalStrategy(0.7, true);
    assertEquals(stats.subList(4, 5), low.evaluate(stats));
    low = new LowCostStableEvalStrategy(0.3, false);
    assertEquals(stats.subList(4, 5), low.evaluate(stats));
    low = new LowCostStableEvalStrategy(0.7, false);
    assertEquals(stats.subList(4, 5), low.evaluate(stats));
  }

  @Test
  public void testMostStable() throws Exception {
    MostStableEvalStrategy most = new MostStableEvalStrategy(false, true);
    assertEquals(stats.subList(3, 4), most.evaluate(stats));
    most = new MostStableEvalStrategy(true, true);
    assertEquals(stats.subList(3, 4), most.evaluate(stats));
    most = new MostStableEvalStrategy(false, false);
    assertEquals(stats.subList(4, 5), most.evaluate(stats));
    most = new MostStableEvalStrategy(true, false);
    assertEquals(stats.subList(4, 5), most.evaluate(stats));
  }
}
