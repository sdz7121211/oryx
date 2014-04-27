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

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Assume;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.als.computation.local.ALSLocalGenerationRunner;
import com.cloudera.oryx.als.serving.ServerRecommender;
import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * Superclass of various tests that exercise the Computation Layer ALS code.
 * 
 * @author Sean Owen
 */
public abstract class AbstractComputationIT extends OryxTest {

  private static final Logger log = LoggerFactory.getLogger(AbstractComputationIT.class);

  static final Path TEST_TEMP_INBOUND_DIR = OryxTest.TEST_TEMP_BASE_DIR.resolve("00000").resolve("inbound");

  private ServerRecommender recommender;

  protected abstract Path getTestDataPath();

  final ServerRecommender getRecommender() {
    return recommender;
  }

  @Override
  protected String getTestConfigResource() {
    return "AbstractComputationIT.conf";
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    Path testDataDir = getTestDataPath();
    Assume.assumeTrue("Skipping test because data is not present", isDirectoryWithFiles(testDataDir));

    Files.createDirectories(TEST_TEMP_INBOUND_DIR);
    log.info("Copying files to {}", TEST_TEMP_INBOUND_DIR);

    for (Path srcDataFile : IOUtils.listFiles(testDataDir)) {
      Path destFile = TEST_TEMP_INBOUND_DIR.resolve(srcDataFile.getFileName());
      Files.copy(srcDataFile, destFile);
    }

    ConfigUtils.overlayConfigOnDefault(getResourceAsFile(getClass().getSimpleName() + ".conf"));

    new ALSLocalGenerationRunner().call();

    recommender = new ServerRecommender(TEST_TEMP_INBOUND_DIR);
    recommender.await();
  }

  @Override
  public void tearDown() throws Exception {
    if (recommender != null) {
      recommender.close();
    }
    super.tearDown();
  }

}
