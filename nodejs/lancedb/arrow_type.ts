// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import {
  type DataType,
  Float32,
  Float64,
  Int8,
  Int16,
  Int32,
  Uint8,
  Uint16,
  Uint32,
} from "apache-arrow";

/**
 * Map a JS TypedArray instance to the corresponding Arrow element type and
 * length. Returns undefined when the view is not a supported TypedArray.
 */
export function typedArrayToArrowType(
  value: ArrayBufferView,
): { elementType: DataType; length: number } | undefined {
  if (value instanceof Float32Array)
    return { elementType: new Float32(), length: value.length };
  if (value instanceof Float64Array)
    return { elementType: new Float64(), length: value.length };
  if (value instanceof Uint8Array)
    return { elementType: new Uint8(), length: value.length };
  if (value instanceof Uint16Array)
    return { elementType: new Uint16(), length: value.length };
  if (value instanceof Uint32Array)
    return { elementType: new Uint32(), length: value.length };
  if (value instanceof Int8Array)
    return { elementType: new Int8(), length: value.length };
  if (value instanceof Int16Array)
    return { elementType: new Int16(), length: value.length };
  if (value instanceof Int32Array)
    return { elementType: new Int32(), length: value.length };
  return undefined;
}
