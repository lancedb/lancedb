# LanceDB Documentation

LanceDB docs are deployed to https://lancedb.github.io/lancedb/.

Docs is built and deployed automatically by [Github Actions](.github/workflows/docs.yml)
whenever a commit is pushed to the `main` branch. So it is possible for the docs to show
unreleased features.

## Building the docs

### Setup
1. Install LanceDB. From LanceDB repo root: `pip install -e python`
2. Install dependencies. From LanceDB repo root: `pip install -r docs/requirements.txt`
3. Make sure you have node and npm setup
4. Make sure protobuf and libssl are installed

### Building node module and create markdown files

See [Javascript docs README](./src/javascript/README.md)

### Build docs
From LanceDB repo root:

Run: `PYTHONPATH=. mkdocs build -f docs/mkdocs.yml`

If successful, you should see a `docs/site` directory that you can verify locally.

### Run local server

You can run a local server to test the docs prior to deployment by navigating to the `docs` directory and running the following command:

```bash
cd docs
mkdocs serve
```

### Run doctest for typescript example

```bash
npm i
npm run build
npm run all
```
