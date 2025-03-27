## Getting help

The following sections provide various diagnostics and troubleshooting tips for LanceDB.
These can help you provide additional information when asking questions or making
error reports.

For trouble shooting, the best place to ask is in our Discord, under the relevant
language channel. By asking in the language-specific channel, it makes it more
likely that someone who knows the answer will see your question.

## Common issues

* Multiprocessing with `fork` is not supported. You should use `spawn` instead.

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

To understand how a query was actually executed—including metrics like execution time, number of rows processed, I/O stats, and more—use the analyze_plan method. This executes the query and returns a physical execution plan annotated with runtime metrics, making it especially helpful for performance tuning and debugging.

* Python Sync: [LanceQueryBuilder.analyze_plan][lancedb.query.LanceQueryBuilder.analyze_plan]
* Python Async: [AsyncQueryBase.analyze_plan][lancedb.query.AsyncQueryBase.analyze_plan]
* Node @lancedb/lancedb: [LanceQueryBuilder.analyzePlan](/lancedb/js/classes/QueryBase/#analyzePlan)
