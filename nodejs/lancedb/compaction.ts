// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import type { OptimizeResult } from "./optimize";

/** @deprecated Use OptimizeResult. */
export type CompactionResult = OptimizeResult;
export { decodeOptimizeResult as decodeCompactionResult } from "./optimize";
