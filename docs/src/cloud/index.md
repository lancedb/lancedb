# About LanceDB Cloud

LanceDB Cloud is a SaaS (software-as-a-service) solution that runs serverless in the cloud, clearly separating storage from compute. It's designed to be cost-effective and highly scalable without breaking the bank. LanceDB Cloud is currently in private beta with general availability coming soon, but you can apply for early access with the private beta release by signing up below.

[Try out LanceDB Cloud](https://noteforms.com/forms/lancedb-mailing-list-cloud-kty1o5?notionforms=1&utm_source=notionforms){ .md-button .md-button--primary }

## Architecture

LanceDB Cloud provides the same underlying fast vector store that powers the OSS version, but without the need to maintain your own infrastructure. Because it's serverless, you only pay for the storage you use, and you can scale compute up and down as needed depending on the size of your data and its associated index.

![](../assets/lancedb_cloud.png)

## Transitioning from the OSS to the Cloud version

The OSS version of LanceDB is designed to be embedded in your application, and it runs in-process. This makes it incredibly simple to self-host your own AI retrieval workflows for RAG and more and build and test out your concepts on your own infrastructure. The OSS version is forever free, and you can continue to build and integrate LanceDB into your existing backend applications without any added costs.

Should you decide that you need a managed deployment in production, it's possible to seamlessly transition from the OSS to the cloud version by changing the connection string to point to a remote database instead of a local one. With LanceDB Cloud, you can take your AI application from development to production without major code changes or infrastructure burden.
