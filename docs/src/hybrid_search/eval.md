# Hybrid Search

Hybrid Search is a broad (often misused) term. It can mean anything from combining multiple methods for searching, to applying ranking methods to better sort the results. In this blog, we use the definition of "hybrid search" to mean using a combination of keyword-based and vector search.

## The challenge of (re)ranking search results
Once you have a group of the most relevant search results from multiple search sources, you'd likely standardize the score and rank them accordingly. This process can also be seen as another independent step - reranking.
There are two approaches for reranking search results from multiple sources.
* <b>Score-based</b>: Calculate final relevance scores based on a weighted linear combination of individual search algorithm scores. Example - Weighted linear combination of semantic search & keyword-based search results.
* <b>Relevance-based</b>: Discards the existing scores and calculates the relevance of each search result - query pair. Example - Cross Encoder models

Even though there are many strategies for reranking search results, none works for all cases. Moreover, evaluating them itself is a challenge. Also, reranking can be dataset, application specific so it's hard to generalize.

### Example evaluation of hybrid search with Reranking

Here's some evaluation numbers from experiment comparing these re-rankers on about 800 queries. It is modified version of an evaluation script from [llama-index](https://github.com/run-llama/finetune-embedding/blob/main/evaluate.ipynb) that measures hit-rate at top-k.

<b> With OpenAI ada2 embedding </b>

Vector Search baseline - `0.64`

| Reranker | Top-3 | Top-5 | Top-10 |
| --- | --- | --- | --- |
| Linear Combination | `0.73` | `0.74` | `0.85` |
| Cross Encoder | `0.71` | `0.70` | `0.77` |
| Cohere | `0.81` | `0.81` | `0.85` |
| ColBERT | `0.68` | `0.68` | `0.73` |

<p>
<img src="https://github.com/AyushExel/assets/assets/15766192/d57b1780-ef27-414c-a5c3-73bee7808a45">
</p>

<b> With OpenAI embedding-v3-small </b>

Vector Search baseline - `0.59`

| Reranker | Top-3 | Top-5 | Top-10 |
| --- | --- | --- | --- |
| Linear Combination | `0.68` | `0.70` | `0.84` |
| Cross Encoder | `0.72` | `0.72` | `0.79` |
| Cohere | `0.79` | `0.79` | `0.84` |
| ColBERT | `0.70` | `0.70` | `0.76` |

<p>
<img src="https://github.com/AyushExel/assets/assets/15766192/259adfd2-6ec6-4df6-a77d-1456598970dd">
</p>

### Conclusion

The results show that the reranking methods are able to improve the search results. However, the improvement is not consistent across all rerankers. The choice of reranker depends on the dataset and the application. It is also important to note that the reranking methods are not a replacement for the search methods. They are complementary and should be used together to get the best results. The speed to recall tradeoff is also an important factor to consider when choosing the reranker.
