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

import { Float, Float32 } from "../arrow";
import { EmbeddingFunction } from "./embedding_function";
import { register } from "./registry";

export type XenovaTransformerOptions = {
	model: string;
};

@register("xenova")
export class XenovaTransformer extends EmbeddingFunction<
	string,
	Partial<XenovaTransformerOptions>
> {
	#model?: import("@xenova/transformers").PreTrainedModel;
	#tokenizer?: import("@xenova/transformers").PreTrainedTokenizer;
	#modelName: XenovaTransformerOptions["model"];
	#initialized = false;

	constructor(
		options: Partial<XenovaTransformerOptions> = {
			model: "Xenova/all-MiniLM-L6-v2",
		},
	) {
		super();

		const modelName = options?.model ?? "Xenova/all-MiniLM-L6-v2";

		this.#modelName = modelName;
	}

	async init() {
		let transformers;
		try {
			// SAFETY:
			// since typescript transpiles `import` to `require`, we need to do this in an unsafe way
			// We can't use `require` because `@xenova/transformers` is an ESM module
			// and we can't use `import` directly because typescript will transpile it to `require`.
			// and we want to remain compatible with both ESM and CJS modules
			// so we use `eval` to bypass typescript for this specific import.
			transformers = await eval('import("@xenova/transformers")');
		} catch (e) {
			throw new Error(
				"error loading @xenova/transformers. Make sure you have installed the package",
			);
		}

		try {
			this.#model = await transformers.AutoModel.from_pretrained(
				this.#modelName,
			);
		} catch (e) {
			throw new Error(
				`error loading model ${this.#modelName}. Make sure you are using a wasm compatible model`,
			);
		}
		try {
			this.#tokenizer = await transformers.AutoTokenizer.from_pretrained(
				this.#modelName,
			);
		} catch (e) {
			throw new Error(
				`error loading tokenizer for ${this.#modelName}. Make sure you are using a wasm compatible model`,
			);
		}
		this.#initialized = true;
	}

	toJSON() {
		return {
			model: this.#modelName,
		};
	}

	ndims(): number {
		return 384;
	}

	embeddingDataType(): Float {
		return new Float32();
	}

	async computeSourceEmbeddings(data: string[]): Promise<number[][]> {
		if (!this.#initialized) {
			await this.init();
		}
		const tokenizer = this.#tokenizer;
		const model = this.#model;
		if (!tokenizer || !model) {
			throw new Error(
				"Model or Tokenizer not initialized, something went wrong",
			);
		}

		const inputs = await tokenizer(data, { padding: true });
		let tokens: import("@xenova/transformers").Tensor = (
			await model.forward(inputs)
		)["last_hidden_state"];
		const [nItems, nTokens] = tokens.dims;

		tokens = tensorDiv(tokens.sum(1), nTokens);

		// TODO: support other data types
		// biome-ignore lint/suspicious/noExplicitAny: <explanation>
		const tokenData: Float32Array = <any>tokens.data;
		const stride = tokens.stride()[0];
		const embeddings = [];
		for (let i = 0; i < nItems; i++) {
			const start = i * stride;
			const end = start + stride;
			const slice = tokenData.slice(start, end);
			embeddings.push(Array.from(slice)); // TODO: use slice directly
		}

		return embeddings;
	}

	async computeQueryEmbeddings(data: string): Promise<number[]> {
		return (await this.computeSourceEmbeddings([data]))[0];
	}
}

const tensorDiv = (
	src: import("@xenova/transformers").Tensor,
	divBy: number,
) => {
	for (let i = 0; i < src.data.length; ++i) {
		src.data[i] /= divBy;
	}
	return src;
};
