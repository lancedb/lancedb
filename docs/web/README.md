# LanceDB documentation

The open-source pages of [docs.lancedb.com](https://docs.lancedb.com), beside the
code they describe.

## Preview locally

```bash
npm i -g mint      # https://mintlify.com/docs/installation
cd docs/web && mint dev
```

That serves this directory on its own. Enterprise pages live in a private
repository and are merged in when the published site is assembled, so links to
`/enterprise/...` and the SQL and Geneva sections will not resolve here. Every
open-source page does.

## Code examples

Examples are not written into the pages. They live in real tests under
`docs/web-tests/{py,ts,rs}`, are extracted into `docs/web/snippets/`, and are
imported by the pages. Edit the test, then regenerate:

```bash
uv run docs/web-tests/mdx_snippets_gen.py -s docs/web-tests/py
```

An example that does not compile fails the pull request that broke it.

## Section anchors

Every heading carries an explicit `{#anchor}`. Those anchors are how the
Enterprise pages attach their additions to the right section, so they are
assigned once and not regenerated — leave an existing one alone even if the
heading it sits on is reworded.
