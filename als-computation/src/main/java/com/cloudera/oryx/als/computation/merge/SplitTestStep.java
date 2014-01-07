/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.als.computation.merge;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Channels;
import org.apache.crunch.types.avro.Avros;

import java.io.IOException;

import com.cloudera.oryx.als.computation.ALSJobStep;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.computation.common.JobStepConfig;

/**
 * @author Sean Owen
 */
public final class SplitTestStep extends ALSJobStep {

  @Override
  protected MRPipeline createPipeline() throws IOException {

    JobStepConfig jobConfig = getConfig();

    String instanceDir = jobConfig.getInstanceDir();
    int generationID = jobConfig.getGenerationID();

    String trainKey = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID) + "train/";
    String testKey = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID) + "test/";

    if (!validOutputPath(trainKey)) {
      return null;
    }

    MRPipeline p = createBasicPipeline(SplitTestStep.class);

    String inboundKey = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID) + "inbound/";

    double testSetFraction = ConfigUtils.getDefaultConfig().getDouble("model.test-set-fraction");

    PCollection<Pair<String, String>> split = p.read(textInput(inboundKey))
        .parallelDo("splitTest", new SplitTestFn(testSetFraction),
            Avros.pairs(Avros.strings(), Avros.strings()));

    Pair<PCollection<String>,PCollection<String>> trainTest = Channels.split(split);
    PCollection<String> train = trainTest.first();
    PCollection<String> test = trainTest.second();

    train.write(compressedTextOutput(p.getConfiguration(), trainKey));
    test.write(compressedTextOutput(p.getConfiguration(), testKey));

    return p;
  }

  public static void main(String[] args) throws Exception {
    run(new SplitTestStep(), args);
  }

}
