# Variable and Secrets

Most embedding configuration options are saved in the table's metadata. However,
this isn't always appropriate. For example, API keys should never be stored in the
metadata. Additionally, other configuration options might be best set at runtime,
such as the `device` configuration that controls whether to use GPU or CPU for
inference. If you hardcoded this to GPU, you wouldn't be able to run the code on
a server without one.

To handle these cases, you can set variables on the embedding registry and
reference them in the embedding configuration. These variables will be available
during the runtime of your program, but not saved in the table's metadata. When
the table is loaded from a different process, the variables must be set again.

To set a variable, use the `set_var()` / `setVar()` method on the embedding registry.
To reference a variable, use the syntax `$env:VARIABLE_NAME`. If there is a default
value, you can use the syntax `$env:VARIABLE_NAME:DEFAULT_VALUE`.

## Using variables to set secrets

Sensitive configuration, such as API keys, must either be set as environment
variables or using variables on the embedding registry. If you pass in a hardcoded
value, LanceDB will raise an error. Instead, if you want to set an API key via
configuration, use a variable:

=== "Python"

    ```python
    --8<-- "python/python/tests/docs/test_embeddings_optional.py:register_secret"
    ```

=== "Typescript"

    ```typescript
    --8<-- "nodejs/examples/embedding.test.ts:register_secret"
    ```

## Using variables to set the device parameter

Many embedding functions that run locally have a `device` parameter that controls
whether to use GPU or CPU for inference. Because not all computers have a GPU,
it's helpful to be able to set the `device` parameter at runtime, rather than
have it hard coded in the embedding configuration. To make it work even if the
variable isn't set, you could provide a default value of `cpu` in the embedding
configuration.

Some embedding libraries even have a method to detect which devices are available,
which could be used to dynamically set the device at runtime. For example, in Python
you can check if a CUDA GPU is available using `torch.cuda.is_available()`.

```python
--8<-- "python/python/tests/docs/test_embeddings_optional.py:register_device"
```
