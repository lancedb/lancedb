# Agent Skills

This directory contains repo-scoped code agent skills for the LanceDB project.

Each skill is a folder that contains a required `SKILL.md` and optional bundled resources.

Codex discovers skills from `.agents/skills` in the current working directory and parent directories.

The `lancedb` skill lives in the `plugins/lancedb` plugin (see `plugins/lancedb/skills/lancedb`)
so it can be installed via the plugin marketplaces (`.claude-plugin/marketplace.json` and
`.agents/plugins/marketplace.json`); the `lancedb` entry here is a symlink into that plugin.
