// Copyright 2023 Lance Developers.
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

import { describe } from 'mocha'
import { assert } from 'chai'

import { OpenAIEmbeddingFunction } from '../../embedding/openai'
import { isEmbeddingFunction } from '../../embedding/embedding_function'

// eslint-disable-next-line @typescript-eslint/no-var-requires
const OpenAIApi = require('openai')
// eslint-disable-next-line @typescript-eslint/no-var-requires
const { stub } = require('sinon')

describe('OpenAPIEmbeddings', function () {
  const stubValue = {
    data: [
      {
        embedding: Array(1536).fill(1.0)
      },
      {
        embedding: Array(1536).fill(2.0)
      }
    ]
  }

  describe('#embed', function () {
    it('should create vector embeddings', async function () {
      const openAIStub = stub(OpenAIApi.Embeddings.prototype, 'create').returns(stubValue)
      const f = new OpenAIEmbeddingFunction('text', 'sk-key')
      const vectors = await f.embed(['abc', 'def'])
      assert.isTrue(openAIStub.calledOnce)
      assert.equal(vectors.length, 2)
      assert.deepEqual(vectors[0], stubValue.data[0].embedding)
      assert.deepEqual(vectors[1], stubValue.data[1].embedding)
    })
  })

  describe('isEmbeddingFunction', function () {
    it('should match the isEmbeddingFunction guard', function () {
      assert.isTrue(isEmbeddingFunction(new OpenAIEmbeddingFunction('text', 'sk-key')))
    })
  })
})
