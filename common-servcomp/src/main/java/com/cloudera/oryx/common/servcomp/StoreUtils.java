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

package com.cloudera.oryx.common.servcomp;

import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Utilities that are generally used but not quite core storage operations as in {@link Store}.
 * 
 * @author Sean Owen
 */
public final class StoreUtils {

  private static final Logger log = LoggerFactory.getLogger(StoreUtils.class);

  private static final Splitter ON_DELIMITER = Splitter.on('/');

  private StoreUtils() {
  }

  /**
   * Lists all generation keys for a given instance. The results are ordered by recency -- most recent generation
   * is last. Note that this not in every case the same as lexicographic ordering, although almost always is.
   * It will not in the case of generation number "wrapping around": 00000,99998,99999 will be returned as
   * 99998,99999,00000.
   * 
   * @param instanceDir instance directory from which to retrieve generations for
   * @return locations of all generation directories for the given instance
   */
  public static List<String> listGenerationsForInstance(String instanceDir) throws IOException {
    List<String> generations = Store.get().list(Namespaces.getInstancePrefix(instanceDir), false);
    orderGenerations(generations);
    return generations;
  }

  /**
   * @param generations lexicographically ordered list of generations
   * @return generations ordered by creation -- accounts for the case of 00000,99998,99999 where 00000 is
   *  most recent
   */
  public static List<String> orderGenerations(List<String> generations) throws IOException {
    if (generations.isEmpty()) {
      return generations;
    }
    String highestGenerationKey = generations.get(generations.size() - 1);
    int highestGeneration = parseGenerationFromPrefix(highestGenerationKey);
    if (highestGeneration < Namespaces.MAX_GENERATION) {
      // Nothing to do to order these further
      return generations;
    }

    Store store = Store.get();
    long lastModified = store.getLastModified(highestGenerationKey);
    for (int i = 0; i < generations.size(); i++) {
      String possiblyLaterGeneration = generations.get(i);
      long possiblyLaterModTime = store.getLastModified(possiblyLaterGeneration);
      if (possiblyLaterModTime <= lastModified) {
        Collections.rotate(generations, -i);
        break;
      }
      lastModified = possiblyLaterModTime;
    }
    return generations;
  }

  public static int parseGenerationFromPrefix(CharSequence prefix) {
    try {
      return Integer.parseInt(lastNonEmptyDelimited(prefix));
    } catch (NumberFormatException nfe) {
      log.error("Bad generation directory: {}", prefix);
      throw nfe;
    }
  }

  public static String lastNonEmptyDelimited(CharSequence path) {
    String result = null;
    for (String s : ON_DELIMITER.split(path)) {
      if (!s.isEmpty()) {
        result = s;
      }
    }
    return result;
  }
  
}
