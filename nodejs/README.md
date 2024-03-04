# (New) LanceDB NodeJS SDK

It will replace the NodeJS SDK when it is ready.

## Development

```sh
npm run build
npm t
```

### Running lint / format

LanceDb uses eslint for linting. VSCode does not need any plugins to use eslint. However, it
may need some additional configuration. Make sure that eslint.experimental.useFlatConfig is
set to true. Also, if your vscode root folder is the repo root then you will need to set
the eslint.workingDirectories to ["nodejs"]. To manually lint your code you can run:

```sh
npm run lint
```

LanceDb uses prettier for formatting. If you are using VSCode you will need to install the
"Prettier - Code formatter" extension. You should then configure it to be the default formatter
for typescript and you should enable format on save. To manually check your code's format you
can run:

```sh
npm run chkformat
```

If you need to manually format your code you can run:

```sh
npx prettier --write .
```

### Generating docs

```sh
npm run docs

cd ../docs
# Asssume the virtual environment was created
# python3 -m venv venv
# pip install -r requirements.txt
. ./venv/bin/activate
mkdocs build
```
