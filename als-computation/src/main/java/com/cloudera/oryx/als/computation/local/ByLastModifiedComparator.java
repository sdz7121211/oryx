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

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * Compares {@link Path} objects by their last modified time. It orders by
 * time ascending.
 *
 * @author Sean Owen
 */
final class ByLastModifiedComparator implements Comparator<Path>, Serializable {

  static final Comparator<Path> INSTANCE = new ByLastModifiedComparator();

  private ByLastModifiedComparator() {
  }

  @Override
  public int compare(Path a, Path b) {
    try {
      return Files.getLastModifiedTime(a).compareTo(Files.getLastModifiedTime(b));
    } catch (IOException ioe) {
      throw new IllegalStateException(ioe);
    }
  }

}
