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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipInputStream;

/**
 * Simple utility methods related to I/O.
 *
 * @author Sean Owen
 */
public final class IOUtils {

  /**
   * A {@link DirectoryStream.Filter} that accepts files whose name does not start with "."
   */
  private static final DirectoryStream.Filter<Path> NOT_HIDDEN = new DirectoryStream.Filter<Path>() {
    private final Pattern pattern = Pattern.compile("^[^.].*");
    @Override
    public boolean accept(Path entry) {
      return pattern.matcher(entry.getFileName().toString()).matches();
    }
  };

  private IOUtils() {
  }

  /**
   * Attempts to recursively delete a directory. This may not work across symlinks.
   *
   * @param rootDir directory to delete along with contents
   * @throws IOException if any deletion fails
   */
  public static void deleteRecursively(Path rootDir) throws IOException {
    if (rootDir == null || !Files.exists(rootDir)) {
      return;
    }
    Files.walkFileTree(rootDir, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }
      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  /**
   * Opens an {@link InputStream} to the file. If it appears to be compressed, because its file name ends in
   * ".gz" or ".zip", then it will be decompressed accordingly
   *
   * @param file file, possibly compressed, to open
   * @return {@link InputStream} on uncompressed contents
   * @throws IOException if the stream can't be opened or is invalid or can't be read
   */
  public static InputStream openMaybeDecompressing(Path file) throws IOException {
    String name = file.getFileName().toString();
    InputStream in = Files.newInputStream(file);
    if (name.endsWith(".gz")) {
      return new GZIPInputStream(in);
    }
    if (name.endsWith(".zip")) {
      return new ZipInputStream(in);
    }
    return in;
  }
  
  /**
   * @param file file, possibly compressed, to open
   * @return {@link Reader} on uncompressed contents
   * @throws IOException if the stream can't be opened or is invalid or can't be read
   * @see #openMaybeDecompressing(Path)
   */
  public static Reader openReaderMaybeDecompressing(Path file) throws IOException {
    return new InputStreamReader(openMaybeDecompressing(file), StandardCharsets.UTF_8);
  }

  /**
   * @param delegate {@link OutputStream} to wrap
   * @return a {@link GZIPOutputStream} wrapping the given {@link OutputStream}
   */
  public static GZIPOutputStream buildGZIPOutputStream(OutputStream delegate) throws IOException {
    return new GZIPOutputStream(delegate, true);
  }

  /**
   * @param delegate {@link OutputStream} to wrap
   * @return the result of {@link #buildGZIPOutputStream(OutputStream)} as a {@link Writer} that encodes
   *  using UTF-8 encoding
   */
  public static Writer buildGZIPWriter(OutputStream delegate) throws IOException {
    return new OutputStreamWriter(buildGZIPOutputStream(delegate), StandardCharsets.UTF_8);
  }

  /**
   * @see #buildGZIPWriter(OutputStream)
   */
  public static Writer buildGZIPWriter(Path path) throws IOException {
    return buildGZIPWriter(Files.newOutputStream(path));
  }

  /**
   * Wraps its argument in {@link BufferedReader} if not already one.
   */
  public static BufferedReader buffer(Reader maybeBuffered) {
    return maybeBuffered instanceof BufferedReader 
        ? (BufferedReader) maybeBuffered 
        : new BufferedReader(maybeBuffered);
  }

  /**
   * @return true iff the given file is a gzip-compressed file with no content; the file itself may not
   *  be empty because it contains gzip headers and footers
   * @throws IOException if the file is not a gzip file or can't be read
   */
  public static boolean isGZIPFileEmpty(Path path) throws IOException {
    try (InputStream in = new GZIPInputStream(Files.newInputStream(path))) {
      return in.read() == -1;
    }
  }

  /**
   * @param prefix prefix to use in name of created file
   * @param suffix file extension
   * @return temporary file
   * @throws IOException if an error occurs while creating the temp file
   */
  public static Path createTempFile(String prefix, String suffix) throws IOException {
    Path temp = Files.createTempFile(prefix, suffix);
    temp.toFile().deleteOnExit();
    return temp;
  }

  /**
   * @param prefix prefix to use in name of created directory
   * @return a temp directory that will auto delete on JVM exit
   * @throws IOException if an error occurs while creating the temp dir
   */
  public static Path createTempDirectory(String prefix) throws IOException {
    Path temp = Files.createTempDirectory(prefix);
    temp.toFile().deleteOnExit();
    return temp;
  }

  /**
   * @param dir directory to list
   * @return regular non-hidden files in the directory
   * @throws IOException if an error occurs while listing the directory
   */
  public static List<Path> listFiles(Path dir) throws IOException {
    List<Path> paths = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, NOT_HIDDEN)) {
      for (Path path : stream) {
        if (Files.isRegularFile(path)) {
          paths.add(path);
        }
      }
    }
    return paths;
  }

}
