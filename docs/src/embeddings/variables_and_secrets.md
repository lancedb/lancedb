# Variable and Secrets

Most embedding configuration options are saved in the table's metadata. However,
this isn't always appropriate. For example, API keys should never be stored in the
metadata. Additionally, other configuration options might be best set at runtime,
such as the `device` configuration that controls whether to use GPU or CPU for
inference. If you hardcoded this to GPU, you wouldn't be able to run the code on
server without one.

To handle these cases, you can set variables on the embedding registry and
reference them in the embedding configuration. These variables will be available
during the runtime of your program, but not saved in the table's metadata. When
the table is loaded from a different process, the variables must be set again
for the embedding functions to be able to be loaded.

To set a variable, use the `set_var()` / `setVar()` method on the embedding registry.
To reference a variable, use the syntax `$env:VARIABLE_NAME`. If there is a default
value, you can use the syntax `$env:VARIABLE_NAME:DEFAULT_VALUE`.

## Using variables to set secrets

Sensitive configuration, such as API keys, must either be set as environment
variables or using variables on the embedding registry. If you pass in a hardcoded
value, LanceDB will raise an error.

=== "Python"

    ```shell
    --8<-- "python/python/tests/docs/test_embeddings_optional.py:register_secret"
    ```

=== "Typescript"

    ```shell
    --8<-- "docs/src/basic_legacy.ts:create_table"
    ```

## Using variables to set device

Because not all computers have a GPU, it's helpful to be able to set the `device`
parameter at runtime, rather than have it hard coded in the embedding configuration.
To make it work even if the variable isn't set, you could provide a default value
of `cpu` in the embedding configuration.

Some embedding libraries have a method to detect which devices are available,
which could also be used to dynamically set the device at runtime. For example, in Python you can check if a CUDA GPU is available using `torch.cuda.is_available()`.

=== "Python"

    ```shell
    --8<-- "python/python/tests/docs/test_embeddings_optional.py:register_device"
    ```

=== "Typescript"

    ```shell
    --8<-- "docs/src/basic_legacy.ts:create_table"
    ```
