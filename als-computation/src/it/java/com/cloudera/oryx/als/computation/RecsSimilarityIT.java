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

package com.cloudera.oryx.als.computation;

import org.junit.Test;

import java.io.File;

import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * Tests use of {@code model.recommend.compute} and {@code model.item-similarity.compute}.
 * 
 * @author Sean Owen
 */
public final class RecsSimilarityIT extends AbstractComputationIT {

  @Override
  protected File getTestDataPath() {
    return getResourceAsFile("highlambda");
  }

  @Test
  public void testRecommendations() throws Exception {
    File baseDir = new File(ConfigUtils.getDefaultConfig().getString("model.instance-dir"), "00000");
    File recommendDir = new File(baseDir, "recommend");
    assertTrue(recommendDir.exists());
    assertTrue(recommendDir.isDirectory());
    assertTrue(recommendDir.listFiles().length > 0);
  }

  @Test
  public void testItemSimilarity() throws Exception {
    File baseDir = new File(ConfigUtils.getDefaultConfig().getString("model.instance-dir"), "00000");
    File similarityDir = new File(baseDir, "similarItems");
    assertTrue(similarityDir.exists());
    assertTrue(similarityDir.isDirectory());
    assertTrue(similarityDir.listFiles().length > 0);
  }

}
