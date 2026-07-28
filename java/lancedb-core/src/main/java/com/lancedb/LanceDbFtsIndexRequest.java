/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lancedb;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.lance.namespace.model.CreateTableIndexRequest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * FTS index request with a resolved custom stop-word snapshot.
 *
 * <p>Custom stop words replace the built-in list for {@link #getLanguage()} and only affect
 * tokenization when {@link #getRemoveStopWords()} is {@code true}. A {@code null} list keeps the
 * built-in language list. An empty list explicitly replaces it with no stop words.
 *
 * <p>Use exactly one source: {@link #setCustomStopWords(List)} for an inline list or {@link
 * #setCustomStopWordsFile(Path)} for a newline-delimited UTF-8 file on the client. Exact empty
 * strings are removed and exact duplicates retain their first occurrence. Other content, including
 * case and surrounding whitespace, is preserved verbatim. File lines use LF or CRLF terminators; a
 * lone carriage return is part of the stop word.
 *
 * <p>The file is read when {@link LanceDbFtsIndexClient#createTableIndex(LanceDbFtsIndexRequest)}
 * is first called. The resulting owned list is then reused, so retries cannot silently pick up a
 * different file. Client-local paths are never serialized.
 *
 * <p>Do not pass this request to the JNI-backed {@code LanceNamespace.createTableIndex} method.
 * Its upstream generated request model does not yet contain {@code custom_stop_words}, and the JNI
 * layer would discard the extension field. Use {@link LanceDbFtsIndexClient}.
 *
 * <p>A LanceDB table source is not exposed here because the current Java SDK has no embedded/local
 * Table API from which it could prove and materialize a complete snapshot. Remote table references
 * are intentionally not sent to the server.
 */
public final class LanceDbFtsIndexRequest extends CreateTableIndexRequest {
  public static final String JSON_PROPERTY_CUSTOM_STOP_WORDS = "custom_stop_words";

  private enum StopWordsSource {
    NONE,
    INLINE,
    FILE
  }

  private List<String> customStopWords;

  @JsonIgnore private transient StopWordsSource stopWordsSource = StopWordsSource.NONE;
  @JsonIgnore private transient Path customStopWordsFile;
  @JsonIgnore private transient boolean fileSnapshotResolved;

  /** Create an FTS request. The index type is always {@code FTS}. */
  public LanceDbFtsIndexRequest() {
    super.setIndexType("FTS");
  }

  /**
   * Return the resolved custom stop-word snapshot.
   *
   * <p>For a file source this remains {@code null} until the FTS index client resolves the file.
   *
   * @return {@code null} for the built-in language list, or the immutable custom snapshot
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_CUSTOM_STOP_WORDS)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public List<String> getCustomStopWords() {
    return customStopWords;
  }

  /**
   * Set inline custom stop words.
   *
   * @param words custom stop words, {@code null} to use the built-in language list, or an empty
   *     list to explicitly use no stop words
   * @throws IllegalArgumentException if an element is null or is not a string at runtime
   * @throws IllegalStateException if a file source is already configured
   */
  @JsonProperty(JSON_PROPERTY_CUSTOM_STOP_WORDS)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setCustomStopWords(@javax.annotation.Nullable List<String> words) {
    if (words == null) {
      clearCustomStopWords();
      return;
    }
    requireCompatibleSource(StopWordsSource.INLINE);
    customStopWords = snapshot(words);
    customStopWordsFile = null;
    fileSnapshotResolved = false;
    stopWordsSource = StopWordsSource.INLINE;
  }

  /**
   * Fluent form of {@link #setCustomStopWords(List)}.
   *
   * @param words inline stop words
   * @return this request
   */
  public LanceDbFtsIndexRequest customStopWords(@javax.annotation.Nullable List<String> words) {
    setCustomStopWords(words);
    return this;
  }

  /**
   * Configure a newline-delimited UTF-8 file source.
   *
   * <p>The path is client-local and is not serialized. It is read exactly once, on the first call
   * to the FTS index client. Pass {@code null} to clear the source and use the built-in language
   * list.
   *
   * @param path client-local stop-word file
   * @throws IllegalArgumentException if the path is empty
   * @throws IllegalStateException if an inline source is already configured
   */
  @JsonIgnore
  public void setCustomStopWordsFile(@javax.annotation.Nullable Path path) {
    if (path == null) {
      clearCustomStopWords();
      return;
    }
    if (path.toString().isEmpty()) {
      throw new IllegalArgumentException(
          "custom stop words file source requires a non-empty path");
    }
    requireCompatibleSource(StopWordsSource.FILE);
    customStopWords = null;
    customStopWordsFile = path;
    fileSnapshotResolved = false;
    stopWordsSource = StopWordsSource.FILE;
  }

  /**
   * Fluent form of {@link #setCustomStopWordsFile(Path)}.
   *
   * @param path client-local stop-word file
   * @return this request
   */
  @JsonIgnore
  public LanceDbFtsIndexRequest customStopWordsFile(@javax.annotation.Nullable Path path) {
    setCustomStopWordsFile(path);
    return this;
  }

  /**
   * Keep this specialized request constrained to FTS.
   *
   * @param indexType must be {@code FTS}
   */
  @Override
  public void setIndexType(String indexType) {
    if (!"FTS".equals(indexType)) {
      throw new IllegalArgumentException("LanceDbFtsIndexRequest index type must be FTS");
    }
    super.setIndexType(indexType);
  }

  /**
   * Keep this specialized request constrained to FTS.
   *
   * @param indexType must be {@code FTS}
   * @return this request
   */
  @Override
  public LanceDbFtsIndexRequest indexType(String indexType) {
    setIndexType(indexType);
    return this;
  }

  void resolveCustomStopWordsSnapshot() throws IOException {
    if (stopWordsSource != StopWordsSource.FILE || fileSnapshotResolved) {
      return;
    }

    byte[] bytes;
    try {
      bytes = Files.readAllBytes(customStopWordsFile);
    } catch (IOException e) {
      throw new IOException(
          "failed to read custom stop words file `" + customStopWordsFile + "`: " + e.getMessage(),
          e);
    }

    String contents;
    try {
      contents =
          StandardCharsets.UTF_8
              .newDecoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT)
              .decode(ByteBuffer.wrap(bytes))
              .toString();
    } catch (CharacterCodingException e) {
      throw new IOException(
          "custom stop words file `" + customStopWordsFile + "` is not valid UTF-8", e);
    }

    customStopWords = snapshot(splitLines(contents));
    fileSnapshotResolved = true;
  }

  /**
   * Match Rust {@code str::lines()}: split only on LF and remove a CR only when it belongs to a
   * CRLF terminator. A lone CR is stop-word content and must be preserved.
   */
  private static List<String> splitLines(String contents) {
    List<String> lines = new ArrayList<>();
    int lineStart = 0;
    for (int index = 0; index < contents.length(); index++) {
      if (contents.charAt(index) != '\n') {
        continue;
      }
      int lineEnd = index;
      if (lineEnd > lineStart && contents.charAt(lineEnd - 1) == '\r') {
        lineEnd--;
      }
      lines.add(contents.substring(lineStart, lineEnd));
      lineStart = index + 1;
    }
    if (lineStart < contents.length()) {
      lines.add(contents.substring(lineStart));
    }
    return lines;
  }

  private void clearCustomStopWords() {
    customStopWords = null;
    customStopWordsFile = null;
    fileSnapshotResolved = false;
    stopWordsSource = StopWordsSource.NONE;
  }

  private void requireCompatibleSource(StopWordsSource requested) {
    if (stopWordsSource != StopWordsSource.NONE && stopWordsSource != requested) {
      throw new IllegalStateException(
          "custom stop words inline and file sources are mutually exclusive; clear the "
              + stopWordsSource.name().toLowerCase(Locale.ROOT)
              + " source before configuring a "
              + requested.name().toLowerCase(Locale.ROOT)
              + " source");
    }
  }

  private static List<String> snapshot(List<String> words) {
    Set<String> seen = new LinkedHashSet<>();
    for (int index = 0; index < words.size(); index++) {
      Object value = words.get(index);
      if (value == null) {
        throw new IllegalArgumentException(
            "custom stop words inline value at index " + index + " cannot be null");
      }
      if (!(value instanceof String)) {
        throw new IllegalArgumentException(
            "custom stop words inline value at index "
                + index
                + " must be a string, but was "
                + value.getClass().getName());
      }
      String word = (String) value;
      if (!word.isEmpty()) {
        seen.add(word);
      }
    }
    return Collections.unmodifiableList(new ArrayList<>(seen));
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof LanceDbFtsIndexRequest)) {
      return false;
    }
    LanceDbFtsIndexRequest that = (LanceDbFtsIndexRequest) other;
    return super.equals(other)
        && Objects.equals(customStopWords, that.customStopWords)
        && stopWordsSource == that.stopWordsSource
        && Objects.equals(customStopWordsFile, that.customStopWordsFile)
        && fileSnapshotResolved == that.fileSnapshotResolved;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        customStopWords,
        stopWordsSource,
        customStopWordsFile,
        fileSnapshotResolved);
  }
}
