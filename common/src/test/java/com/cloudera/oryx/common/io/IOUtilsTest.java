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

package com.cloudera.oryx.common.io;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.cloudera.oryx.common.OryxTest;
import org.junit.Test;

/**
 * Tests {@link IOUtils}.
 *
 * @author Sean Owen
 */
public final class IOUtilsTest extends OryxTest {

  private static final byte[] SOME_BYTES = { 0x01, 0x02, 0x03 };

  @Test
  public void testDeleteRecursively() throws IOException {
    Path tempDir = IOUtils.createTempDirectory("temp");
    assertTrue(Files.exists(tempDir));
    Path subFile1 = tempDir.resolve("subFile1");
    Files.write(subFile1, SOME_BYTES);
    assertTrue(Files.exists(subFile1));
    Path subDir1 = tempDir.resolve("subDir1");
    Files.createDirectories(subDir1);
    Path subFile2 = subDir1.resolve("subFile2");
    Files.write(subFile2, SOME_BYTES);
    assertTrue(Files.exists(subFile2));
    Path subDir2 = subDir1.resolve("subDir2");
    Files.createDirectories(subDir2);

    IOUtils.deleteRecursively(tempDir);

    assertTrue(Files.notExists(tempDir));
    assertTrue(Files.notExists(subFile1));
    assertTrue(Files.notExists(subDir1));
    assertTrue(Files.notExists(subFile2));
    assertTrue(Files.notExists(subDir2));
  }

}
