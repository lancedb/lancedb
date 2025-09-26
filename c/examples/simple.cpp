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

constexpr size_t DIM = 128;

auto create_schema() {
  // use arrow to define schema: [id, item]
  auto id_field = arrow::field("id", arrow::int32());
  auto item_field = arrow::field("item", arrow::fixed_size_list(arrow::float32(), DIM));
  return arrow::schema({id_field, item_field});
}

auto create_some_records() {
  arrow::Int32Builder id_builder;
  arrow::FixedSizeListBuilder item_builder(arrow::default_memory_pool(),
      std::make_unique<arrow::FloatBuilder>(),
      DIM);

  // add sample rows
  constexpr size_t TOTAL = 100;
  for (size_t i = 0; i < TOTAL; i++) {
    // key column
    id_builder.Append(i).ok();
    // data column
    auto list_builder = static_cast<arrow::FloatBuilder*>(item_builder.value_builder());
    for (size_t j = 0; j < DIM; j++) {
      list_builder->Append(0.1).ok();
    }
    item_builder.Append().ok();
  }

  // build arrays
  std::shared_ptr<arrow::Array> id_array, item_array;
  id_builder.Finish(&id_array).ok();
  item_builder.Finish(&item_array).ok();

  return arrow::RecordBatch::Make(create_schema(), TOTAL, {id_array, item_array});
}

LanceDBTable* create_table(LanceDBConnection* db) {
  const std::string table_name = "my_table";
  auto schema = create_schema();
  struct ArrowSchema c_schema;
  if (const auto status = arrow::ExportSchema(*schema, &c_schema); !status.ok()) {
    std::cerr << "failed to export schema to C ABI: " << status.ToString() << std::endl;
    return nullptr;
  }

  auto initial_data = create_some_records();
  struct ArrowArray c_array;
  if (const auto status = arrow::ExportRecordBatch(*initial_data, &c_array, &c_schema); !status.ok()) {
    std::cerr << "failed to export record batch to C ABI: " << status.ToString() << std::endl;
    return nullptr;
  }

  LanceDBRecordBatchReader* reader = lancedb_record_batch_reader_from_arrow(
      reinterpret_cast<FFI_ArrowArray*>(&c_array),
      reinterpret_cast<FFI_ArrowSchema*>(&c_schema));
  if (!reader) {
    std::cerr << "failed to create record batch reader from arrow arrays" << std::endl;
    return nullptr;
  }

  LanceDBTable* tbl;
  if (const LanceDBError result = lancedb_table_create(db, table_name.c_str(),
        reinterpret_cast<FFI_ArrowSchema*>(&c_schema),
        reader, &tbl, nullptr); result != LANCEDB_SUCCESS) {
    std::cerr << "error creating table: " << table_name << ", error: " << lancedb_error_to_message(result) << std::endl;
    lancedb_record_batch_reader_free(reader);
    return nullptr;
  }

  std::cout << "created table: " << table_name << " (with data)" << std::endl;

  auto new_data = create_some_records();
  if (const auto status = arrow::ExportRecordBatch(*initial_data, &c_array, &c_schema); !status.ok()) {
    std::cerr << "failed to export record batch to C ABI: " << status.ToString() << std::endl;
    return nullptr;
  }

  reader = lancedb_record_batch_reader_from_arrow(
      reinterpret_cast<FFI_ArrowArray*>(&c_array),
      reinterpret_cast<FFI_ArrowSchema*>(&c_schema));
  if (!reader) {
    std::cerr << "failed to create record batch reader from arrow arrays" << std::endl;
    if (c_array.release) {
      c_array.release(&c_array);
    }
    return nullptr;
  }
  if (const LanceDBError result = lancedb_table_add(tbl, reader, nullptr); result != LANCEDB_SUCCESS) {
    std::cerr << "failed to write record batch to table, error: " << lancedb_error_to_message(result) << std::endl;
    lancedb_record_batch_reader_free(reader);
  }
  std::cout << "wrote rows to table" << std::endl;

  if (c_schema.release) {
    c_schema.release(&c_schema);
  }
  return tbl;
}

LanceDBTable* create_empty_table(LanceDBConnection* db) {
  // convert arrow C++ schema to arrow C ABI
  auto schema = create_schema();
  struct ArrowSchema c_schema;
  if (const auto status = arrow::ExportSchema(*schema, &c_schema); !status.ok()) {
    std::cerr << "failed to export schema to C ABI: " << status.ToString() << std::endl;
    return nullptr;
  }

  // create an empty table based on the schema
  const std::string table_name = "empty_table";
  LanceDBTable* tbl;
  if (const LanceDBError result = lancedb_table_create(db, table_name.c_str(),
        reinterpret_cast<FFI_ArrowSchema*>(&c_schema),
        nullptr, &tbl, nullptr); result != LANCEDB_SUCCESS) {
    std::cerr << "error creating table: " << table_name << ", error: " << lancedb_error_to_message(result) << std::endl;
    lancedb_connection_free(db);
    return nullptr;
  }
  std::cout << "created table: " << table_name << " (empty)" << std::endl;
  return tbl;
}

void create_index(LanceDBTable* tbl) {
  // define a vector index on the "item" column
  const char* data_columns[] = {"item"};
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
    std::cerr << "failed to create vector index on 'item' column, error: " << lancedb_error_to_message(result) << std::endl;
    return;
  }
  std::cout << "created vector index on 'data' column" << std::endl;
}

using SearchResult = std::pair<struct ArrowArray**,struct ArrowSchema*>;

SearchResult search(LanceDBTable* tbl) {
  // query the table using the nearest_to function
  const size_t k = 2;
  size_t count_out;
  float vector[DIM] = {0.1};
  struct ArrowArray** c_arrays;
  struct ArrowSchema* c_schema;
  if (const LanceDBError result = lancedb_table_nearest_to(
        tbl,
        vector,
        DIM,
        k, // top k
        "item",
        reinterpret_cast<FFI_ArrowArray***>(&c_arrays),
        reinterpret_cast<FFI_ArrowSchema**>(&c_schema),
        &count_out, nullptr); result != LANCEDB_SUCCESS) {
    std::cerr << "error querying nearest to vector, error: " << lancedb_error_to_message(result) << std::endl;
    return {nullptr, nullptr};
  }
  std::cout << "query returned " << count_out << " results" << std::endl;
  return {c_arrays, c_schema};
}

void print_query_result(
    struct ArrowArray** c_arrays_ptr,
    struct ArrowSchema* c_schema_ptr) {
  if (auto schema = arrow::ImportSchema(c_schema_ptr); schema.ok()) {
    std::cout << "result schema:" << std::endl << (*schema)->ToString() << std::endl;
    if (auto array = arrow::ImportRecordBatch(
          reinterpret_cast<struct ArrowArray*>(*c_arrays_ptr),
          *schema); array.ok()) {
      for (const auto& col : (*array)->columns()) {
        std::cout << col->ToString() << std::endl;
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

  // list table names
  char** table_names;
  size_t name_count;
  if (const LanceDBError result = lancedb_connection_table_names(db, &table_names, &name_count, nullptr); result != LANCEDB_SUCCESS) {
    std::cerr << "error listing table names, error: " << lancedb_error_to_message(result) << std::endl;
  } else {
    std::cout << name_count << " tables found" << std::endl;
    for (size_t i = 0; i < name_count; i++) {
      std::cout << "table: " << table_names[i] << std::endl;
    }
    lancedb_free_table_names(table_names, name_count);
  }

  auto tbl = create_table(db);
  create_index(tbl);
  auto [c_arrays, c_schema] = search(tbl);
  print_query_result(c_arrays, c_schema);
  if (const LanceDBError result = lancedb_table_delete(tbl, "id > 24", nullptr); result != LANCEDB_SUCCESS) {
    std::cerr << "error deleting rows from table, error: " << lancedb_error_to_message(result) << std::endl;
  } else {
    std::cout << "deleted rows where id > 24" << std::endl;
  }
  lancedb_table_free(tbl);

  auto empty_table = create_empty_table(db);
  lancedb_table_free(empty_table);

  if (const LanceDBError result = lancedb_connection_drop_table(db, "my_table", nullptr, nullptr); result != LANCEDB_SUCCESS) {
    std::cerr << "error dropping table, error: " << lancedb_error_to_message(result) << std::endl;
  } else {
    std::cout << "dropped table my_table" << std::endl;
  }
  lancedb_connection_free(db);

  return 0;
}

