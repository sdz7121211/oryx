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

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

/**
 * Tests {@link Store}.
 *
 * @author Sean Owen
 */
public final class StoreTest extends OryxTest {

  @Test
  public void testSize() throws Exception {
    Path file = IOUtils.createTempFile("testSize", ".txt");
    Files.write(file, Collections.singleton("Hello."), StandardCharsets.UTF_8);
    assertEquals(7, Store.get().getSize(file.toString()));
  }

  @Test
  public void testSizeRecursive() throws Exception {
    Path dir = IOUtils.createTempDirectory("temp");
    Path subDir = dir.resolve("subdir");
    Files.createDirectories(subDir);
    Path file1 = dir.resolve("testDU1.txt");
    Path file2 = subDir.resolve("testDU2.txt");
    Files.write(file1, Collections.singleton("Hello."), StandardCharsets.UTF_8);
    Files.write(file2, Collections.singleton("Shalom."), StandardCharsets.UTF_8);
    Store store = Store.get();
    assertEquals(7, store.getSizeRecursive(file1.toString()));
    assertEquals(8, store.getSizeRecursive(file2.toString()));
    assertEquals(8, store.getSizeRecursive(subDir.toString()));
    assertEquals(15, store.getSizeRecursive(dir.toString()));
    IOUtils.deleteRecursively(dir);
  }

}
