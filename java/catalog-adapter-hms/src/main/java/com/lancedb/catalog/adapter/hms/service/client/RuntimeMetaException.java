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

import org.apache.hadoop.hive.metastore.api.MetaException;

/** Exception used to wrap {@link MetaException} as a {@link RuntimeException} and add context. */
public class RuntimeMetaException extends RuntimeException {
  public RuntimeMetaException(MetaException cause) {
    super(cause);
  }

  public RuntimeMetaException(MetaException cause, String message, Object... args) {
    super(String.format(message, args), cause);
  }

  public RuntimeMetaException(Throwable throwable, String message, Object... args) {
    super(String.format(message, args), throwable);
  }
}
