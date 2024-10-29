## Getting help

The following sections provide various diagnostics and troubleshooting tips for LanceDB.
These can help you provide additional information when asking questions or making
error reports.

For trouble shooting, the best place to ask is in our Discord, under the relevant
language channel. By asking in the language-specific channel, it makes it more
likely that someone who knows the answer will see your question.

## Enabling logging

To provide more information, especially for LanceDB Cloud related issues, enable
debug logging. You can set the `LANCEDB_LOG` environment variable:

```shell
export LANCEDB_LOG=debug
```

You can turn off colors and formatting in the logs by setting

```shell
export LANCEDB_LOG_STYLE=never
```

## Explaining query plans

If you have slow queries or unexpected query results, it can be helpful to
print the resolved query plan. You can use the `explain_plan` method to do this:

* Python Sync: [LanceQueryBuilder.explain_plan][lancedb.query.LanceQueryBuilder.explain_plan]
* Python Async: [AsyncQueryBase.explain_plan][lancedb.query.AsyncQueryBase.explain_plan]
* Node @lancedb/lancedb: [LanceQueryBuilder.explainPlan](/lancedb/js/classes/QueryBase/#explainplan)
