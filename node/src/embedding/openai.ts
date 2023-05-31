
import { type EmbeddingFunction } from '../index'

export class OpenAIEmbeddingFunction implements EmbeddingFunction<string> {
  private readonly _openai: any
  private readonly _modelName: string

  constructor (sourceColumn: string, openAIKey: string, modelName: string = 'text-embedding-ada-002') {
    let openai
    try {
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      openai = require('openai')
    } catch {
      throw new Error('please install openai using npm install openai')
    }

    this.sourceColumn = sourceColumn
    const configuration = new openai.Configuration({
      apiKey: openAIKey
    })
    this._openai = new openai.OpenAIApi(configuration)
    this._modelName = modelName
  }

  async embed (data: string[]): Promise<number[][]> {
    const response = await this._openai.createEmbedding({
      model: this._modelName,
      input: data
    })
    const embeddings: number[][] = []
    for (let i = 0; i < response.data.data.length; i++) {
      embeddings.push(response.data.data[i].embedding as number[])
    }
    return embeddings
  }

  sourceColumn: string
}
