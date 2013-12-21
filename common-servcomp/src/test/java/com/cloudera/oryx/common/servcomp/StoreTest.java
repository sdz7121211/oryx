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

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.io.IOUtils;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.junit.Test;

import java.io.File;

/**
 * Tests {@link Store}.
 *
 * @author Sean Owen
 */
public final class StoreTest extends OryxTest {

  @Test
  public void testSize() throws Exception {
    File file = File.createTempFile("testSize", ".txt");
    file.deleteOnExit();
    Files.write("Hello.", file, Charsets.UTF_8);
    assertEquals(6, Store.get().getSize(file.toString()));
  }

  @Test
  public void testSizeRecursive() throws Exception {
    File dir = Files.createTempDir();
    dir.deleteOnExit();
    File subDir = new File(dir, "subdir");
    assertTrue(subDir.mkdir());
    File file1 = new File(dir, "testDU1.txt");
    File file2 = new File(subDir, "testDU2.txt");
    Files.write("Hello.", file1, Charsets.UTF_8);
    Files.write("Shalom.", file2, Charsets.UTF_8);
    Store store = Store.get();
    assertEquals(6, store.getSizeRecursive(file1.toString()));
    assertEquals(7, store.getSizeRecursive(file2.toString()));
    assertEquals(7, store.getSizeRecursive(subDir.toString()));
    assertEquals(13, store.getSizeRecursive(dir.toString()));
    IOUtils.deleteRecursively(dir);
  }

}
