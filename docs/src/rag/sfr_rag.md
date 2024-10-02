**SFR RAG 📑**
====================================================================
Salesforce AI Research introduces SFR-RAG, a 9-billion-parameter language model trained with a significant emphasis on reliable, precise, and faithful contextual generation abilities specific to real-world RAG use cases and relevant agentic tasks. They include precise factual knowledge extraction, distinguishing relevant against distracting contexts, citing appropriate sources along with answers, producing complex and multi-hop reasoning over multiple contexts, consistent format following, as well as refraining from hallucination over unanswerable queries.

**[Offical Implementation](https://github.com/SalesforceAIResearch/SFR-RAG)**

<figure markdown="span">
  ![agent-based-rag](https://raw.githubusercontent.com/lancedb/assets/main/docs/assets/rag/salesforce_contextbench.png)
  <figcaption>Average Scores in ContextualBench: <a href="https://blog.salesforceairesearch.com/sfr-rag/">Source</a>
  </figcaption>
</figure>

To reliably evaluate LLMs in contextual question-answering for RAG, Saleforce introduced [ContextualBench](https://huggingface.co/datasets/Salesforce/ContextualBench?ref=blog.salesforceairesearch.com), featuring 7 benchmarks like [HotpotQA](https://arxiv.org/abs/1809.09600?ref=blog.salesforceairesearch.com) and [2WikiHopQA](https://www.aclweb.org/anthology/2020.coling-main.580/?ref=blog.salesforceairesearch.com) with consistent setups. 

SFR-RAG outperforms GPT-4o, achieving state-of-the-art results in 3 out of 7 benchmarks, and significantly surpasses Command-R+ while using 10 times fewer parameters. It also excels at handling context, even when facts are altered or conflicting.

[Saleforce AI Research Blog](https://blog.salesforceairesearch.com/sfr-rag/)
