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

package com.cloudera.oryx.als.serving.example;

import java.util.regex.Pattern;

import com.cloudera.oryx.als.common.rescorer.PairRescorer;
import com.cloudera.oryx.als.common.rescorer.Rescorer;

/**
 * Simple example of a {@link Rescorer} / {@link PairRescorer} that filters out everything except IDs
 * matching a given regular expression.
 *
 * @author Sean Owen
 */
public final class RegexRescorer implements Rescorer, PairRescorer {

  private final Pattern pattern;

  public RegexRescorer(Pattern pattern) {
    this.pattern = pattern;
  }

  @Override
  public double rescore(String id, double originalScore) {
    return originalScore; // Assume we already checked whether it was filtered
  }

  @Override
  public boolean isFiltered(String id) {
    return !pattern.matcher(id).matches();
  }

  @Override
  public double rescore(String fromID, String toID, double originalScore) {
    return originalScore; // Assume we already checked whether it was filtered
  }

  @Override
  public boolean isFiltered(String fromID, String toID) {
    return !pattern.matcher(toID).matches();
  }

}
