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

package com.cloudera.oryx.als.computation.local;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;

import com.cloudera.oryx.als.common.StringLongMapping;
import com.cloudera.oryx.als.common.pmml.ALSModelDescription;
import com.cloudera.oryx.common.collection.LongFloatMap;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.collection.LongSet;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.iterator.LongPrimitiveIterator;
import com.cloudera.oryx.common.io.DelimitedDataUtils;

final class WriteOutputs implements Callable<Object> {

  private static final Logger log = LoggerFactory.getLogger(WriteOutputs.class);

  private static final char KEY_VALUE_DELIMITER = '\t'; // Matches Hadoop TextOutputFormat
  private static final String SINGLE_OUT_FILENAME = "0.csv.gz";

  private final Path modelDir;
  private final LongObjectMap<LongFloatMap> RbyRow;
  private final LongObjectMap<LongSet> knownItemIDs;
  private final LongObjectMap<float[]> X;
  private final LongObjectMap<float[]> Y;
  private final StringLongMapping idMapping;

  WriteOutputs(Path modelDir,
               LongObjectMap<LongFloatMap> RbyRow,
               LongObjectMap<LongSet> knownItemIDs,
               LongObjectMap<float[]> X,
               LongObjectMap<float[]> Y,
               StringLongMapping idMapping) {
    this.modelDir = modelDir;
    this.RbyRow = RbyRow;
    this.knownItemIDs = knownItemIDs;
    this.X = X;
    this.Y = Y;
    this.idMapping = idMapping;
  }

  @Override
  public Void call() throws IOException {
    log.info("Writing current input");
    writeCombinedInput(RbyRow, modelDir.resolve("input"));
    log.info("Writing known items");
    writeIDIDsMap(knownItemIDs, modelDir.resolve("knownItems"));
    log.info("Writing X");
    writeIDFloatMap(X, modelDir.resolve("X"));
    log.info("Writing Y");
    writeIDFloatMap(Y, modelDir.resolve("Y"));
    log.info("Writing ID mapping");
    writeMapping(idMapping, modelDir.resolve("idMapping"));
    log.info("Writing model");
    Path modelDescriptionFile = modelDir.resolve("model.pmml.gz");
    ALSModelDescription modelDescription = new ALSModelDescription();
    modelDescription.setKnownItemsPath("knownItems");
    modelDescription.setXPath("X");
    modelDescription.setYPath("Y");
    modelDescription.setIDMappingPath("idMapping");
    ALSModelDescription.write(modelDescriptionFile, modelDescription);
    return null;
  }

  private static void writeCombinedInput(LongObjectMap<LongFloatMap> RbyRow, Path inputDir) throws IOException {
    Path outFile = inputDir.resolve(SINGLE_OUT_FILENAME);
    Files.createDirectories(inputDir);
    log.info("Writing input of {} entries to {}", RbyRow.size(), outFile);
    try (Writer out = IOUtils.buildGZIPWriter(outFile)) {
      for (LongObjectMap.MapEntry<LongFloatMap> row : RbyRow.entrySet()) {
        long rowID = row.getKey();
        for (LongFloatMap.MapEntry entry : row.getValue().entrySet()) {
          long colID = entry.getKey();
          float value = entry.getValue();
          out.write(DelimitedDataUtils.encode(',', Long.toString(rowID), Long.toString(colID), Float.toString(value)));
          out.write('\n');
        }
      }
    }
  }

  private static void writeIDIDsMap(LongObjectMap<LongSet> idIDs, Path idIDsDir) throws IOException {
    if (idIDs == null || idIDs.isEmpty()) {
      return;
    }
    Path outFile = idIDsDir.resolve(SINGLE_OUT_FILENAME);
    Files.createDirectories(idIDsDir);
    log.info("Writing ID-ID map of {} entries to {}", idIDs.size(), outFile);
    try (Writer out = IOUtils.buildGZIPWriter(outFile)) {
      for (LongObjectMap.MapEntry<LongSet> entry : idIDs.entrySet()) {
        out.write(String.valueOf(entry.getKey()));
        out.write(KEY_VALUE_DELIMITER);
        LongSet ids = entry.getValue();
        LongPrimitiveIterator it = ids.iterator();
        Collection<String> keyStrings = new ArrayList<>(ids.size());
        while (it.hasNext()) {
          keyStrings.add(Long.toString(it.nextLong()));
        }
        out.write(DelimitedDataUtils.encode(',', keyStrings));
        out.write('\n');
      }
    }
  }

  private static void writeIDFloatMap(LongObjectMap<float[]> idFloatMap, Path idFloatDir) throws IOException {
    if (idFloatMap.isEmpty()) {
      return;
    }
    Path outFile = idFloatDir.resolve(SINGLE_OUT_FILENAME);
    Files.createDirectories(idFloatDir);
    log.info("Writing ID-float map of {} entries to {}", idFloatMap.size(), outFile);
    try (Writer out = IOUtils.buildGZIPWriter(outFile)) {
      for (LongObjectMap.MapEntry<float[]> entry : idFloatMap.entrySet()) {
        out.write(String.valueOf(entry.getKey()));
        out.write(KEY_VALUE_DELIMITER);
        float[] f = entry.getValue();
        String[] floatStrings = new String[f.length];
        for (int i = 0; i < floatStrings.length; i++) {
          floatStrings[i] = Float.toString(f[i]);
        }
        out.write(DelimitedDataUtils.encode(',', (Object[]) floatStrings));
        out.write('\n');
      }
    }
  }

  private static void writeMapping(StringLongMapping idMapping, Path idMappingDir) throws IOException {
    Path outFile = idMappingDir.resolve(SINGLE_OUT_FILENAME);
    Files.createDirectories(idMappingDir);
    log.info("Writing mapping of {} entries to {}", idMapping.size(), outFile);
    try (Writer out = IOUtils.buildGZIPWriter(outFile)) {
      Lock lock = idMapping.getLock().readLock();
      lock.lock();
      try {
        for (LongObjectMap.MapEntry<String> entry : idMapping.getReverseMapping().entrySet()) {
          out.write(DelimitedDataUtils.encode(',', Long.toString(entry.getKey()), entry.getValue()));
          out.write('\n');
        }
      } finally {
        lock.unlock();
      }
    }
  }

}
