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

import com.cloudera.oryx.common.io.IOUtils;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * Tests use of {@code model.recommend.compute} and {@code model.item-similarity.compute}.
 * 
 * @author Sean Owen
 */
public final class RecsSimilarityIT extends AbstractComputationIT {

  @Override
  protected Path getTestDataPath() {
    return getResourceAsFile("highlambda");
  }

  @Test
  public void testRecommendations() throws Exception {
    Path baseDir = Paths.get(ConfigUtils.getDefaultConfig().getString("model.instance-dir"), "00000");
    Path recommendDir = baseDir.resolve("recommend");
    assertTrue(Files.isDirectory(recommendDir));
    assertFalse(IOUtils.listFiles(recommendDir).isEmpty());
  }

  @Test
  public void testItemSimilarity() throws Exception {
    Path baseDir = Paths.get(ConfigUtils.getDefaultConfig().getString("model.instance-dir"), "00000");
    Path similarityDir = baseDir.resolve("similarItems");
    assertTrue(Files.isDirectory(similarityDir));
    assertFalse(IOUtils.listFiles(similarityDir).isEmpty());
  }

}
