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

import { toSQL } from '../util'
import * as chai from 'chai'

const expect = chai.expect

describe('toSQL', function () {
  it('should turn string to SQL expression', function () {
    expect(toSQL('foo')).to.equal("'foo'")
  })

  it('should turn number to SQL expression', function () {
    expect(toSQL(123)).to.equal('123')
  })

  it('should turn boolean to SQL expression', function () {
    expect(toSQL(true)).to.equal('TRUE')
  })

  it('should turn null to SQL expression', function () {
    expect(toSQL(null)).to.equal('NULL')
  })

  it('should turn Date to SQL expression', function () {
    const date = new Date('05 October 2011 14:48 UTC')
    expect(toSQL(date)).to.equal("'2011-10-05T14:48:00.000Z'")
  })

  it('should turn array to SQL expression', function () {
    expect(toSQL(['foo', 'bar', true, 1])).to.equal("['foo', 'bar', TRUE, 1]")
  })
})
