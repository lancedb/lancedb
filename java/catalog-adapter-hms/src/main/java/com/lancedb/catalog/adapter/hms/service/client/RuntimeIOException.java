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
package com.lancedb.catalog.adapter.hms.service.client;

import java.io.IOException;
import java.io.UncheckedIOException;

/** Exception used to wrap {@link IOException} as a {@link RuntimeException} and add context. */
public class RuntimeIOException extends UncheckedIOException {

  public RuntimeIOException(IOException cause) {
    super(cause);
  }

  public RuntimeIOException(IOException cause, String message, Object... args) {
    super(String.format(message, args), cause);
  }

  public RuntimeIOException(String message, Object... args) {
    super(new IOException(String.format(message, args)));
  }
}
