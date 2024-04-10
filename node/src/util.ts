// Copyright 2023 LanceDB Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

export type Literal = string | number | boolean | null | Date | Literal[]

export function toSQL (value: Literal): string {
  if (typeof value === 'string') {
    return `'${value}'`
  }

  if (typeof value === 'number') {
    return value.toString()
  }

  if (typeof value === 'boolean') {
    return value ? 'TRUE' : 'FALSE'
  }

  if (value === null) {
    return 'NULL'
  }

  if (value instanceof Date) {
    return `'${value.toISOString()}'`
  }

  if (Array.isArray(value)) {
    return `[${value.map(toSQL).join(', ')}]`
  }

  // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
  throw new Error(`Unsupported value type: ${typeof value} value: (${value})`)
}

export class TTLCache {
  private readonly cache: Map<string, { value: any, expires: number }>

  /**
   * @param ttl Time to live in milliseconds
   */
  constructor (private readonly ttl: number) {
    this.cache = new Map()
  }

  get (key: string): any | undefined {
    const entry = this.cache.get(key)
    if (entry === undefined) {
      return undefined
    }

    if (entry.expires < Date.now()) {
      this.cache.delete(key)
      return undefined
    }

    return entry.value
  }

  set (key: string, value: any): void {
    this.cache.set(key, { value, expires: Date.now() + this.ttl })
  }

  delete (key: string): void {
    this.cache.delete(key)
  }
}
