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

package com.cloudera.oryx.als.common;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Defines an ordering on {@link NumericIDValue}s that sorts by value, ascending.
 * (And breaks ties by sorting by item ID, descending then.)
 *
 * @author Sean Owen
 */
final class ByValueAscComparator implements Comparator<NumericIDValue>, Serializable {

  static final Comparator<NumericIDValue> INSTANCE = new ByValueAscComparator();

  private ByValueAscComparator() {
  }

  @Override
  public int compare(NumericIDValue a, NumericIDValue b) {
    int compare = Float.compare(a.getValue(), b.getValue());
    if (compare != 0) {
      return compare;
    }
    // Break ties by item ID, *de*scending. It's rare but at least gives predictable ordering.
    return Long.compare(b.getID(), a.getID());
  }

}
