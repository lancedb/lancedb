// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow_schema::Schema;
use datafusion_physical_plan::projection::ProjectionExec;

pub fn cast_to_table_schema(table_schema: &Schema, input_schema: &Schema) -> Vec<ProjectionExec> {
    // Reject if missing fields

    todo!()
}
