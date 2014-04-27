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

package com.cloudera.oryx.kmeans.computation.local;

import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.Store;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.computation.common.LocalGenerationRunner;
import com.cloudera.oryx.computation.common.summary.Summary;
import com.cloudera.oryx.kmeans.common.ClusterValidityStatistics;
import com.cloudera.oryx.kmeans.common.KMeansEvalStrategy;
import com.cloudera.oryx.kmeans.common.WeightedRealVector;
import com.cloudera.oryx.kmeans.common.pmml.KMeansPMML;
import com.cloudera.oryx.kmeans.computation.evaluate.EvaluationSettings;
import com.cloudera.oryx.kmeans.computation.evaluate.KMeansEvaluationData;
import com.cloudera.oryx.kmeans.computation.pmml.ClusteringModelBuilder;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.math3.linear.RealVector;
import org.dmg.pmml.ClusteringModel;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.Model;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class KMeansLocalGenerationRunner extends LocalGenerationRunner {
  @Override
  protected void runSteps() throws IOException {
    String instanceDir = getInstanceDir();
    int generationID = getGenerationID();
    String generationPrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID);

    Path currentInboundDir = IOUtils.createTempDirectory("currentInbound");
    Path tempOutDir = IOUtils.createTempDirectory("temp");

    try {
      Store store = Store.get();
      store.downloadDirectory(generationPrefix + "inbound/", currentInboundDir);
      Summary summary = new Summarize(currentInboundDir).call();
      if (summary == null) {
        // No summary created, bail out.
        return;
      }
      List<List<RealVector>> foldVecs = new Standardize(currentInboundDir, summary).call();
      List<List<WeightedRealVector>> weighted = new WeightedPointsByFold(foldVecs).call();
      List<KMeansEvaluationData> evalData = new ClusteringEvaluation(weighted).call();
      List<ClusterValidityStatistics> stats = Lists.transform(evalData,
          new Function<KMeansEvaluationData, ClusterValidityStatistics>() {
            @Override
            public ClusterValidityStatistics apply(KMeansEvaluationData input) {
              return input.getClusterValidityStatistics();
            }
          }
      );

      KMeansEvalStrategy evalStrategy = EvaluationSettings.create(ConfigUtils.getDefaultConfig()).getEvalStrategy();
      if (evalStrategy != null) {
        stats = evalStrategy.evaluate(stats);
      }
      ClusteringModelBuilder b = new ClusteringModelBuilder(summary);
      DataDictionary dictionary = b.getDictionary();
      List<Model> models = new ArrayList<>();
      if (stats.size() == 1) {
        ClusterValidityStatistics best = stats.get(0);
        for (KMeansEvaluationData data : evalData) {
          if (best.getK() == data.getK() && best.getReplica() == data.getReplica()) {
            ClusteringModel cm = b.build(data.getName(generationPrefix), data.getBest());
            models.add(cm);
            evalData = ImmutableList.of(data);
            break;
          }
        }
      } else {
        for (KMeansEvaluationData data : evalData) {
          ClusteringModel cm = b.build(data.getName(generationPrefix), data.getBest());
          models.add(cm);
        }
      }

      List<String> statsStrings = Lists.transform(stats, new Function<ClusterValidityStatistics, String>() {
        @Override
        public String apply(ClusterValidityStatistics input) {
          return input.toString();
        }
      });
      Files.write(tempOutDir.resolve("cluster_stats.csv"), statsStrings, StandardCharsets.UTF_8);
      KMeansPMML.write(tempOutDir.resolve("model.pmml.gz"), dictionary, models);
      List<String> assignments = new Assignment(foldVecs, evalData).call();
      if (!assignments.isEmpty()) {
        Path outlierDir = tempOutDir.resolve("outliers");
        Files.createDirectories(outlierDir);
        Files.write(outlierDir.resolve("data.csv"), assignments, StandardCharsets.UTF_8);
      }
      store.uploadDirectory(generationPrefix, tempOutDir, false);
    } finally {
      IOUtils.deleteRecursively(tempOutDir);
    }
  }
}
