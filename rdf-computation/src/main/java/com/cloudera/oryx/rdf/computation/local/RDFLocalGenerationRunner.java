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

package com.cloudera.oryx.rdf.computation.local;

import com.google.common.collect.BiMap;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.Store;
import com.cloudera.oryx.computation.common.LocalGenerationRunner;
import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.pmml.DecisionForestPMML;
import com.cloudera.oryx.rdf.common.tree.DecisionForest;

/**
 * Implements random decision forests in the Computation Layer as local, parallel, non-distributed, non-Hadoop-based
 * process.
 *
 * @author Sean Owen
 */
public final class RDFLocalGenerationRunner extends LocalGenerationRunner {

  @Override
  protected void runSteps() throws IOException {

    String instanceDir = getInstanceDir();
    int generationID = getGenerationID();
    String generationPrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID);
    int lastGenerationID = getLastGenerationID();

    Path currentInputDir = IOUtils.createTempDirectory("currentInput");
    Path tempOutDir = IOUtils.createTempDirectory("temp");

    try {

      Store store = Store.get();
      store.downloadDirectory(generationPrefix + "inbound/", currentInputDir);
      if (lastGenerationID >= 0) {
        store.downloadDirectory(Namespaces.getInstanceGenerationPrefix(instanceDir, lastGenerationID) + "input/",
                                currentInputDir);
      }

      List<Example> examples = new ArrayList<>();
      Map<Integer,BiMap<String,Integer>> columnToCategoryNameToIDMapping = new HashMap<>();
      new ReadInputs(currentInputDir, examples, columnToCategoryNameToIDMapping).call();

      DecisionForest forest = DecisionForest.fromExamplesWithDefault(examples);

      DecisionForestPMML.write(tempOutDir.resolve("model.pmml.gz"), forest, columnToCategoryNameToIDMapping);

      store.uploadDirectory(generationPrefix + "input/", currentInputDir, false);
      store.uploadDirectory(generationPrefix, tempOutDir, false);

    } finally {
      IOUtils.deleteRecursively(currentInputDir);
      IOUtils.deleteRecursively(tempOutDir);
    }
  }

}
