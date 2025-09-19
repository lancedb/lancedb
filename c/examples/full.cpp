/*
 * SPDX-License-Identifier: Apache-2.0
 * SPDX-FileCopyrightText: Copyright The LanceDB Authors
 */

#include <sys/stat.h>
#include <iostream>
#include <memory>
#include <vector>
#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include "lancedb.h"

// helper function to check if directory exists
int directory_exists(const char* path) {
  struct stat st;
  return stat(path, &st) == 0 && S_ISDIR(st.st_mode);
}

// helper function to remove directory recursively
int remove_directory(const char* path) {
  char command[256];
  snprintf(command, sizeof(command), "rm -rf %s", path);
  return system(command);
}

void print_query_result(
    struct ArrowArray** c_arrays_ptr,
    struct ArrowSchema* c_schema_ptr) {
  if (auto schema = arrow::ImportSchema(c_schema_ptr); schema.ok()) {
    std::cout << "result schema:" << std::endl << (*schema)->ToString() << std::endl;
    if (auto array = arrow::ImportRecordBatch(
          reinterpret_cast<struct ArrowArray*>(*c_arrays_ptr),
          *schema); array.ok()) {
      auto rb = *array;
      std::cout << "result record batch has " << rb->num_rows() << " rows and "
        << rb->num_columns() << " columns" << std::endl;

      // Print by rows instead of columns
      for (int64_t row = 0; row < rb->num_rows(); row++) {
        for (int col = 0; col < rb->num_columns(); col++) {
          auto column = rb->column(col);
          auto field = rb->schema()->field(col);

          // Handle different column types
          if (field->type()->id() == arrow::Type::STRING) {
            auto string_array = std::static_pointer_cast<arrow::StringArray>(column);
            if (!string_array->IsNull(row)) {
              std::cout << "\"" << string_array->GetString(row) << "\"";
            } else {
              std::cout << "null";
            }
          } else if (field->type()->id() == arrow::Type::FIXED_SIZE_LIST) {
            auto list_array = std::static_pointer_cast<arrow::FixedSizeListArray>(column);
            if (!list_array->IsNull(row)) {
              auto values = std::static_pointer_cast<arrow::FloatArray>(list_array->values());
              int32_t start = list_array->value_offset(row);
              int32_t length = list_array->value_length();
              std::cout << "[";
              for (int32_t i = 0; i < length; i++) {
                if (i > 0) std::cout << ", ";
                std::cout << values->Value(start + i);
              }
              std::cout << "]";
            } else {
              std::cout << "null";
            }
          } else if (field->type()->id() == arrow::Type::FLOAT) {
            auto float_array = std::static_pointer_cast<arrow::FloatArray>(column);
            if (!float_array->IsNull(row)) {
              std::cout << float_array->Value(row);
            } else {
              std::cout << "null";
            }
          } else {
            std::cout << "Unsupported column type: " << field->type()->ToString();
          }
          if (col < rb->num_columns() - 1) std::cout << ", ";
        }
        std::cout << std::endl;
      }
    } else {
      std::cerr << "error importing result record batch: " << array.status() << std::endl;
    }
  } else {
    std::cerr << "error importing result schema: " << schema.status() << std::endl;
  }
}

int main() {
  // initial cleanup
  const std::string data_dir = "data";
  if (directory_exists(data_dir.c_str())) {
    std::cout << "removing existing directory: " << data_dir << std::endl;
    remove_directory(data_dir.c_str());
  }

  char* error_message;

  // connect to a db
  const std::string uri = data_dir+"/sample-lancedb";
  LanceDBConnectBuilder* builder = lancedb_connect(uri.c_str());
  if (!builder) {
    std::cerr << "failed to create connection builder" << std::endl;
    return 1;
  }
  LanceDBConnection* db = lancedb_connect_builder_execute(builder);
  if (!db) {
    lancedb_connect_builder_free(builder);
    std::cerr << "failed to connect to database" << std::endl;
    return 1;
  }
  if (const char* connected_uri = lancedb_connection_uri(db); connected_uri) {
    std::cout << "connected to URI: " << connected_uri << std::endl;
  } else {
    std::cerr << "failed to get connected URI" << std::endl;
  }

  // use arrow to define schema: [key, vector, tag1, tag2, tag3]
  constexpr size_t dimensions = 8;
  auto key_field = arrow::field("key", arrow::utf8());
  auto float_type = arrow::float32();
  auto data_field = arrow::field("data", arrow::fixed_size_list(float_type, dimensions));
  auto tag1_field = arrow::field("tag1", arrow::utf8());
  auto tag2_field = arrow::field("tag2", arrow::utf8());
  auto tag3_field = arrow::field("tag3", arrow::utf8());
  auto schema = arrow::schema({key_field, data_field, tag1_field, tag2_field, tag3_field});
  std::cout << "arrow schema:" << std::endl << schema->ToString() << std::endl;
  // convert arrow C++ schema to arrow C ABI
  struct ArrowSchema c_schema;
  if (const auto status = arrow::ExportSchema(*schema, &c_schema); !status.ok()) {
    std::cerr << "failed to export schema to C ABI: " << status.ToString() << std::endl;
    lancedb_connection_free(db);
    return 1;
  }

  // create an empty table based on the schema
  const std::string table_name = "my_table";
  LanceDBTable* table = nullptr;
  if (const LanceDBError result = lancedb_table_create(db, table_name.c_str(),
        reinterpret_cast<FFI_ArrowSchema*>(&c_schema),
        nullptr, &table, nullptr); result != LANCEDB_SUCCESS) {
    std::cerr << "error creating table: " << table_name << ", error: " << lancedb_error_to_message(result) << std::endl;
    lancedb_connection_free(db);
    return 1;
  }
  std::cout << "created table: " << table_name << " (empty)" << std::endl;
  lancedb_table_free(table);

  // try to create a table that already exists
  if (const LanceDBError result = lancedb_table_create(db, "my_table",
        reinterpret_cast<FFI_ArrowSchema*>(&c_schema),
        nullptr, &table, &error_message); result != LANCEDB_SUCCESS) {
    std::cout << "failed to create table that already exists (expected), error: '" <<
      lancedb_error_to_message(result) <<
      "' message: " << std::endl << error_message << std::endl;
    lancedb_free_string(error_message);
  } else {
    std::cerr << "created table that already exists - should have failed" << std::endl;
    lancedb_table_free(table);
  }

  // try to create a table with invalid name
  if (const LanceDBError result = lancedb_table_create(db, "invalid table name",
        reinterpret_cast<FFI_ArrowSchema*>(&c_schema),
        nullptr, &table, &error_message); result != LANCEDB_SUCCESS) {
    std::cout << "failed to create table with invalid name (expected), error: '" <<
      lancedb_error_to_message(result) <<
      "' message: " << std::endl << error_message << std::endl;
    lancedb_free_string(error_message);
  } else {
    std::cerr << "created table with invalid name - should have failed" << std::endl;
    lancedb_table_free(table);
  }

  // try to create a table with invalid input (null schema)
  if (const LanceDBError result = lancedb_table_create(db, "invalid_table",
        nullptr,
        nullptr, &table, &error_message); result != LANCEDB_SUCCESS) {
    std::cout << "failed to create table with null schema (expected), error: '" <<
      lancedb_error_to_message(result) <<
      "' message: " << std::endl << error_message << std::endl;
    lancedb_free_string(error_message);
  } else {
    std::cerr << "created table with null schema - should have failed" << std::endl;
    lancedb_table_free(table);
  }

  // open the table to work with it
  LanceDBTable* tbl = lancedb_connection_open_table(db, table_name.c_str());
  if (!tbl) {
    std::cerr << "failed to open table: " << table_name << std::endl;
    lancedb_connection_free(db);
    return 1;
  }

  // define a scalar index on the "key" column
  const char* key_columns[] = {"key"};
  LanceDBScalarIndexConfig scalar_config = {
    .replace = 1,                    // replace existing index
    .force_update_statistics = 0     // don't force update statistics
  };
  if (const LanceDBError result = lancedb_table_create_scalar_index(
        tbl, key_columns, 1, LANCEDB_INDEX_BTREE, &scalar_config, nullptr); result != LANCEDB_SUCCESS) {
    std::cerr << "failed to create scalar index on 'key' column, error: '" <<
      lancedb_error_to_message(result) << "'" << std::endl;
  } else {
    std::cout << "created scalar index on 'key' column" << std::endl;
  }

  // try to create the same index again without replace flag
  scalar_config.replace = 0;
  if (const LanceDBError result = lancedb_table_create_scalar_index(
        tbl, key_columns, 1, LANCEDB_INDEX_BTREE, &scalar_config, &error_message); result != LANCEDB_SUCCESS) {
    std::cout << "failed to create scalar index on 'key' column (expected), error: '" <<
      lancedb_error_to_message(result) <<
      "' message: " << std::endl << error_message << std::endl;
    lancedb_free_string(error_message);
  } else {
    std::cerr << "created scalar index on 'key' column - should have failed" << std::endl;
  }

  // create sample data using arrow builders
  arrow::StringBuilder key_builder;
  arrow::FloatBuilder float_builder;
  arrow::FixedSizeListBuilder data_builder(arrow::default_memory_pool(),
      std::make_unique<arrow::FloatBuilder>(),
      dimensions);
  arrow::StringBuilder tag1_builder, tag2_builder, tag3_builder;
  // add sample rows
  const int num_rows = 100;
  for (int i = 0; i < num_rows; i++) {
    // key column
    key_builder.Append("key_" + std::to_string(i)).ok();
    // vector data column
    auto list_builder = static_cast<arrow::FloatBuilder*>(data_builder.value_builder());
    for (size_t j = 0; j < dimensions; j++) {
      list_builder->Append(static_cast<float>(rand()%100)).ok();
    }
    data_builder.Append().ok();
    // tag columns
    tag1_builder.Append("category_" + std::to_string(i % 3)).ok();
    tag2_builder.Append("type_" + std::to_string(i % 2)).ok();
    tag3_builder.Append("label_" + std::to_string(i % 5)).ok();
  }
  // build arrays
  std::shared_ptr<arrow::Array> key_array, data_array, tag1_array, tag2_array, tag3_array;
  key_builder.Finish(&key_array).ok();
  data_builder.Finish(&data_array).ok();
  tag1_builder.Finish(&tag1_array).ok();
  tag2_builder.Finish(&tag2_array).ok();
  tag3_builder.Finish(&tag3_array).ok();

  auto record_batch = arrow::RecordBatch::Make(
      schema, num_rows, {key_array, data_array, tag1_array, tag2_array, tag3_array});
  struct ArrowArray c_array;
  if (const auto status = arrow::ExportRecordBatch(*record_batch, &c_array, &c_schema); !status.ok()) {
    std::cerr << "failed to export record batch to C ABI: " << status.ToString() << std::endl;
  } else {
    if (auto batch_reader = lancedb_record_batch_reader_from_arrow(
          reinterpret_cast<FFI_ArrowArray*>(&c_array),
          reinterpret_cast<FFI_ArrowSchema*>(&c_schema)); batch_reader) {
      // add data to table
      if (const LanceDBError result = lancedb_table_add(tbl, batch_reader, nullptr); result != LANCEDB_SUCCESS) {
        std::cerr << "failed to write record batch to table, error: " << lancedb_error_to_message(result) << std::endl;
      } else {
        std::cout << "wrote " << num_rows << " rows to table" << std::endl;
      }
    } else {
      std::cerr << "failed to create record batch reader from arrow arrays" << std::endl;
      lancedb_record_batch_reader_free(batch_reader);
    }
  }

  // define a vector index on the "data" column
  const char* data_columns[] = {"data"};
  LanceDBVectorIndexConfig vector_config = {
    .num_partitions = -1,            // auto
    .num_sub_vectors = -1,           // auto
    .max_iterations = -1,            // default
    .sample_rate = 0.0f,             // default
    .distance_type = LANCEDB_DISTANCE_L2,  // L2 distance
    .accelerator = nullptr,          // CPU
    .replace = 1                     // replace existing index
  };
  if (const LanceDBError result = lancedb_table_create_vector_index(
        tbl, data_columns, 1, LANCEDB_INDEX_IVF_FLAT, &vector_config, nullptr); result != LANCEDB_SUCCESS) {
    std::cerr << "failed to create vector index on 'data' column, error: " << lancedb_error_to_message(result) << std::endl;
  } else {
    std::cout << "created vector index on 'data' column" << std::endl;
  }

  // define bitmap indices on the tag columns
  const char* tag_columns[] = {"tag1", "tag2", "tag3"};
  LanceDBScalarIndexConfig bitmap_config = {
    .replace = 1,                    // replace existing index
    .force_update_statistics = 0     // don't force update statistics
  };
  for (size_t i = 0; i < 3; i++) {
    if (const LanceDBError result = lancedb_table_create_scalar_index(
          tbl, &tag_columns[i], 1, LANCEDB_INDEX_BITMAP, &bitmap_config, nullptr); result != LANCEDB_SUCCESS) {
      std::cerr << "failed to create bitmap index on '" << tag_columns[i] << "' column, error: " << lancedb_error_to_message(result) << std::endl;
    } else {
      std::cout << "created bitmap index on '" << tag_columns[i] << "' column" << std::endl;
    }
  }

  // query the table using the nearest_to function
  const size_t k = 20;
  size_t count_out;
  float vector[dimensions];
  std::cout << "querying nearest to vector: [";
  for (size_t i = 0; i < dimensions; ++i) {
    vector[i] = static_cast<float>(rand()%100);
    std::cout << vector[i] << (i == dimensions - 1 ? "" : ", ");
  }
  std::cout << "]" << std::endl;
  struct ArrowArray** c_arrays_ptr;
  struct ArrowSchema* c_schema_ptr;
  if (const LanceDBError result = lancedb_table_nearest_to(
        tbl,
        vector,
        dimensions,
        k, // top k
        "data",
        reinterpret_cast<FFI_ArrowArray***>(&c_arrays_ptr),
        reinterpret_cast<FFI_ArrowSchema**>(&c_schema_ptr),
        &count_out, nullptr); result != LANCEDB_SUCCESS) {
    std::cerr << "error querying nearest to vector, error: " << lancedb_error_to_message(result) << std::endl;
  } else {
    std::cout << "query returned " << count_out << " results" << std::endl;
    print_query_result(c_arrays_ptr, c_schema_ptr);
  }

  // query the table using query object
  if (LanceDBVectorQuery* query = lancedb_vector_query_new(tbl, vector, dimensions); query) {
    // set limit
    if (const LanceDBError result = lancedb_vector_query_limit(query, k, nullptr); result != LANCEDB_SUCCESS) {
      std::cerr << "error setting limit for query, error: " << lancedb_error_to_message(result) << std::endl;
    } else {
      std::cout << "set query limit to: " << k << std::endl;
      // set vector column
      if (const LanceDBError result = lancedb_vector_query_column(query, "data", nullptr); result != LANCEDB_SUCCESS) {
        std::cerr << "error setting vector column for query, error: " << lancedb_error_to_message(result) << std::endl;
      } else {
        std::cout << "set query vector column to: data" << std::endl;
        // set projection
        const char* columns[] = {"key", "data", "tag1", "tag2"};
        if (const LanceDBError result = lancedb_vector_query_select(query, columns, 4, nullptr); result != LANCEDB_SUCCESS) {
          std::cerr << "error setting projection for query, error: " << lancedb_error_to_message(result) << std::endl;
        } else {
          std::cout << "set query projection to: key, data, tag1, tag2" << std::endl;
          // add filter
          const char* filter = "tag1 = \"category_1\" AND tag2 = \"type_0\"";
          if (LanceDBError result = lancedb_vector_query_where_filter(query, filter, nullptr); result != LANCEDB_SUCCESS) {
            std::cerr << "error adding filter to query, error: " << lancedb_error_to_message(result) << std::endl;
          } else {
            std::cout << "added filter to query: " << filter << std::endl;
            // add distance type
            if (const LanceDBError result = lancedb_vector_query_distance_type(query, LANCEDB_DISTANCE_L2, nullptr); result != LANCEDB_SUCCESS) {
              std::cerr << "error setting distance type for query, error: " << lancedb_error_to_message(result) << std::endl;
            } else {
              std::cout << "set query distance type to: L2" << std::endl;
              // execute the query
              if (LanceDBQueryResult* query_result = lancedb_vector_query_execute(query); query_result) {
                std::cout << "executed query" << std::endl;
                // get the result as arrow arrays
                struct ArrowArray** c_arrays_ptr;
                struct ArrowSchema* c_schema_ptr;
                size_t count_out;
                if (const LanceDBError result = lancedb_query_result_to_arrow(
                      query_result,
                      reinterpret_cast<FFI_ArrowArray***>(&c_arrays_ptr),
                      reinterpret_cast<FFI_ArrowSchema**>(&c_schema_ptr),
                      &count_out,
                      nullptr); result != LANCEDB_SUCCESS) {
                  std::cerr << "error converting query result to arrow, error: " << lancedb_error_to_message(result) << std::endl;
                  lancedb_query_result_free(query_result);
                } else {
                  std::cout << "query returned " << count_out << " results" << std::endl;
                  print_query_result(c_arrays_ptr, c_schema_ptr);
                  lancedb_free_arrow_arrays(reinterpret_cast<FFI_ArrowArray**>(c_arrays_ptr), 1);
                  lancedb_free_arrow_schema(reinterpret_cast<FFI_ArrowSchema*>(c_schema_ptr));
                }
              } else {
                std::cerr << "error executing query" << std::endl;
              }
            }
          }
        }
      }
    }
  }

  lancedb_table_free(tbl);

  // list all tables in the database and loop through them
  char** table_names;
  size_t name_count;
  if (const LanceDBError result = lancedb_connection_table_names(db, &table_names, &name_count, nullptr); result != LANCEDB_SUCCESS) {
    std::cerr << "error listing table names, error: " << lancedb_error_to_message(result) << std::endl;
  } else {
    std::cout << name_count << " tables found" << std::endl;
    for (size_t i = 0; i < name_count; i++) {
      if (LanceDBTable* tbl = lancedb_connection_open_table(db, table_names[i]); tbl) {

        // get the schema of the table
        struct ArrowSchema* c_schema_ptr;
        if (const LanceDBError result = lancedb_table_arrow_schema(
              tbl,
              reinterpret_cast<FFI_ArrowSchema**>(&c_schema_ptr),
              nullptr); result == LANCEDB_SUCCESS) {
          if (auto schema = arrow::ImportSchema(c_schema_ptr); schema.ok()) {
            std::cout << "table: " << table_names[i] << ", schema:" << std::endl;
            std::cout << (*schema)->ToString() << std::endl;
          } else {
            std::cerr << "error converting table schema to arrow schema: " << schema.status() << std::endl;
          }
          lancedb_free_arrow_schema(reinterpret_cast<FFI_ArrowSchema*>(c_schema_ptr));
        } else {
          std::cerr << "error getting schema for table: " << table_names[i] << ", error: " << lancedb_error_to_message(result) << std::endl;
        }

        // list all indices of the table
        char** indices;
        size_t indices_count;
        if (const LanceDBError result = lancedb_table_list_indices(tbl, &indices, &indices_count, nullptr); result != LANCEDB_SUCCESS) {
          std::cerr << "failed to list indices, error: " << lancedb_error_to_message(result) << std::endl;
        } else {
          std::cout << "found " << indices_count << " indices:" << std::endl;
          for (size_t i = 0; i < indices_count; i++) {
            std::cout << "  - " << indices[i] << std::endl;
            // delete the index
            if (const LanceDBError result = lancedb_table_drop_index(tbl, indices[i], nullptr); result != LANCEDB_SUCCESS) {
              std::cerr << "    error dropping index: " << indices[i] << ", error: " << lancedb_error_to_message(result) << std::endl;
            } else {
              std::cout << "    dropped index: " << indices[i] << std::endl;
            }
          }
          lancedb_free_index_list(indices, indices_count);
        }

        // optimize the table after index deletion
        if (const LanceDBError result = lancedb_table_optimize(tbl, LANCEDB_OPTIMIZE_ALL, nullptr); result != LANCEDB_SUCCESS) {
          std::cerr << "error optimizing table: " << table_names[i] << ", error: " << lancedb_error_to_message(result) << std::endl;
        } else {
          std::cout << "optimized table: " << table_names[i] << std::endl;
        }

        // number of rows in the table
        auto row_count = lancedb_table_count_rows(tbl);
        std::cout << "table: " << table_names[i] << " has: " << row_count << " rows" << std::endl;

        // delete some rows
        const auto delete_predicates = {"key = \"key_10\"", "key = \"key_20\"", "key = \"key_30\"", "key = \"kaboom\""};
        for (const auto& predicate : delete_predicates) {
          if (const LanceDBError result = lancedb_table_delete(tbl, predicate, nullptr); result != LANCEDB_SUCCESS) {
            std::cerr << "error deleting row with predicate: " << predicate << ", error: " << lancedb_error_to_message(result) << std::endl;
          } else {
            std::cout << "deleted row with predicate: " << predicate << std::endl;
          }
        }
        // check number of rows in the table after deletion
        row_count = lancedb_table_count_rows(tbl);
        std::cout << "after deletion table: " << table_names[i] << " has: " << row_count << " rows" << std::endl;

        // perform table upsert with 3 new rows and 3 updated rows
        arrow::StringBuilder key_builder;
        arrow::FloatBuilder float_builder;
        arrow::FixedSizeListBuilder data_builder(arrow::default_memory_pool(),
            std::make_unique<arrow::FloatBuilder>(),
            dimensions);
        arrow::StringBuilder tag1_builder, tag2_builder, tag3_builder;
        // add sample rows
        // key columns
        key_builder.Append("key_1" + std::to_string(i)).ok();
        key_builder.Append("key_2" + std::to_string(i)).ok();
        key_builder.Append("key_3" + std::to_string(i)).ok();
        key_builder.Append("key_999" + std::to_string(i)).ok();
        key_builder.Append("key_9999" + std::to_string(i)).ok();
        key_builder.Append("key_99999" + std::to_string(i)).ok();
        const int num_rows = 6;
        for (int i = 0; i < num_rows; i++) {
          // vector data column
          auto list_builder = static_cast<arrow::FloatBuilder*>(data_builder.value_builder());
          for (size_t j = 0; j < dimensions; j++) {
            list_builder->Append(static_cast<float>(rand()%100)).ok();
          }
          data_builder.Append().ok();
          // tag columns
          tag1_builder.Append("category_" + std::to_string(i % 3)).ok();
          tag2_builder.Append("type_" + std::to_string(i % 2)).ok();
          tag3_builder.Append("label_" + std::to_string(i % 5)).ok();
        }
        // build arrays
        std::shared_ptr<arrow::Array> key_array, data_array, tag1_array, tag2_array, tag3_array;
        key_builder.Finish(&key_array).ok();
        data_builder.Finish(&data_array).ok();
        tag1_builder.Finish(&tag1_array).ok();
        tag2_builder.Finish(&tag2_array).ok();
        tag3_builder.Finish(&tag3_array).ok();

        auto record_batch = arrow::RecordBatch::Make(
            schema, num_rows, {key_array, data_array, tag1_array, tag2_array, tag3_array});
        struct ArrowArray c_array;
        if (const auto status = arrow::ExportRecordBatch(*record_batch, &c_array, &c_schema); !status.ok()) {
          std::cerr << "failed to export record batch to C ABI: " << status.ToString() << std::endl;
        } else {
          if (auto batch_reader = lancedb_record_batch_reader_from_arrow(
                reinterpret_cast<FFI_ArrowArray*>(&c_array),
                reinterpret_cast<FFI_ArrowSchema*>(&c_schema)); batch_reader) {
            // upsert the new data to table
            std::array<const char*, 1> on_columns = {"key"};
            //
            const LanceDBMergeInsertConfig config{.when_matched_update_all = 1,
              .when_not_matched_insert_all = 1};
            if (const LanceDBError result = lancedb_table_merge_insert(
                  tbl,
                  batch_reader,
                  on_columns.data(),
                  1,
                  &config,
                  &error_message); result != LANCEDB_SUCCESS) {
              std::cerr << "failed to upsert record batch to table, error: " << lancedb_error_to_message(result) << ", message: " << error_message << std::endl;
            } else {
              std::cout << "upserted " << num_rows << " rows to table" << std::endl;
            }
          } else {
            std::cerr << "failed to create record batch reader from arrow arrays" << std::endl;
            lancedb_record_batch_reader_free(batch_reader);
          }
        }

        // check number of rows in the table after upsert
        row_count = lancedb_table_count_rows(tbl);
        std::cout << "after upsert table: " << table_names[i] << " has: " << row_count << " rows" << std::endl;

        // drop the table
        if (LanceDBError result = lancedb_connection_drop_table(db, table_names[i], nullptr, nullptr); result != LANCEDB_SUCCESS) {
          std::cerr << "error dropping table: " << table_names[i] << ", error: " << lancedb_error_to_message(result) << std::endl;
        } else {
          std::cout << "dropped table: " << table_names[i] << std::endl;
        }
        lancedb_table_free(tbl);
      } else {
        std::cerr << "error opening table: " << table_names[i] << std::endl;
      }
    }
  }

  // TODO: test lancedb_connection_rename_table()
  if (c_schema.release) {
    c_schema.release(&c_schema);
  }

  lancedb_free_table_names(table_names, name_count);
  if (const LanceDBError result = lancedb_connection_drop_all_tables(db, nullptr, nullptr); result != LANCEDB_SUCCESS) {
    std::cerr << "error dropping all tables, error: " << lancedb_error_to_message(result) << std::endl;
  } else {
    std::cout << "dropped all tables" << std::endl;
  }

  lancedb_connection_free(db);

  return 0;
}

