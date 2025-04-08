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

public interface ClientPool<C, E extends Exception> {
  interface Action<R, C, E extends Exception> {
    R run(C client) throws E;
  }

  <R> R run(Action<R, C, E> action) throws E, InterruptedException;

  <R> R run(Action<R, C, E> action, boolean retry) throws E, InterruptedException;
}
