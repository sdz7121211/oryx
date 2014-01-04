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

import java.util.Map;
import java.util.regex.Pattern;

import com.cloudera.oryx.als.common.OryxRecommender;
import com.cloudera.oryx.als.common.rescorer.PairRescorer;
import com.cloudera.oryx.als.common.rescorer.Rescorer;
import com.cloudera.oryx.als.common.rescorer.RescorerProvider;

/**
 * Simple example of a {@link RescorerProvider} that filters out everything except IDs matching a given
 * regular expression.
 *
 * @author Sean Owen
 */
public final class RegexRescorerProvider implements RescorerProvider {

  private final Map<String,Pattern> typeToPattern;

  /**
   * @param typeToPattern mapping from a request "type" to a {@link Pattern} to be used for rescoring the results.
   *  "type" is the first argument sent as a rescorer parameter ({@code rescorerParams=...}). If not present or
   *  maps to no regex then no rescoring will occur.
   */
  public RegexRescorerProvider(Map<String,Pattern> typeToPattern) {
    this.typeToPattern = typeToPattern;
  }

  @Override
  public Rescorer getRecommendRescorer(String[] userIDs, OryxRecommender recommender, String... args) {
    return getRescorer(args);
  }

  @Override
  public Rescorer getRecommendToAnonymousRescorer(String[] itemIDs, OryxRecommender recommender, String... args) {
    return getRescorer(args);
  }

  @Override
  public Rescorer getMostPopularItemsRescorer(OryxRecommender recommender, String... args) {
    return getRescorer(args);
  }

  @Override
  public PairRescorer getMostSimilarItemsRescorer(OryxRecommender recommender, String... args) {
    return getRescorer(args);
  }

  private RegexRescorer getRescorer(String... args) {
    if (args.length == 0) {
      return null;
    }
    String type = args[0];
    Pattern pattern = typeToPattern.get(type);
    if (pattern == null) {
      return null;
    }
    return new RegexRescorer(pattern);
  }

}
