
# CLI & Config

## LanceDB CLI
Once lanceDB is installed, you can access the CLI using `lancedb` command on the console.

```
lancedb
```

This lists out all the various command-line options available. You can get the usage or help for a particular command.

```
lancedb {command} --help
```

## LanceDB config
LanceDB uses a global config file to store certain settings. These settings are configurable using the lanceDB cli.
To view your config settings, you can use:

```
lancedb config
```

These config parameters can be tuned using the cli.

```
lancedb {config_name} --{argument}
```

## LanceDB Opt-in Diagnostics
When enabled, LanceDB will send anonymous events to help us improve LanceDB. These diagnostics are used only for error reporting and no data is collected. Error & stats allow us to automate certain aspects of bug reporting, prioritization of fixes and feature requests.
These diagnostics are opt-in and can be enabled or disabled using the `lancedb diagnostics` command. These are enabled by default.

### Get usage help

```
lancedb diagnostics --help
```

### Disable diagnostics

```
lancedb diagnostics --disabled
```

### Enable diagnostics

```
lancedb diagnostics --enabled
```
