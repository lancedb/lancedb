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

package com.lancedb.lancedb;

import io.questdb.jar.jni.JarJniLoader;
import java.util.List;
 
/**
 * Represents LanceDB database.
*/
public class Database {
  static {
    JarJniLoader.loadLib(Database.class, "/nativelib", "lancedb_jni");
  }

  /**
  * Get the names of all tables in the database.
  *
  * @param databaseUri The URI of the LanceDB database.
  * @return the table names
  */
  public static native List<String> tableNames(String databaseUri);
}