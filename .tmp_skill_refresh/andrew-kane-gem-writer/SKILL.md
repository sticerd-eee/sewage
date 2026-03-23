---
name: andrew-kane-gem-writer
description: Write or refactor Ruby gems in Andrew Kane's style. Use when creating a gem, tightening a gem API, extracting reusable library code, or adding Rails integration with minimal dependencies, explicit Ruby, and Minitest.
---

# Andrew Kane Gem Writer

## Goal

Write gems with simple public APIs, minimal runtime dependencies, explicit code, and optional Rails integration without Rails coupling.

## Core Rules

- Prefer plain Ruby over metaprogramming.
- Keep runtime dependencies at zero or near zero.
- Load Rails conditionally. Never require Rails frameworks eagerly.
- Store configuration on the top-level module with `class << self`.
- Validate inputs early with direct errors.
- Use Minitest. Do not introduce RSpec unless the codebase already depends on it.

## Workflow

1. Inspect the gem shape before changing it.
2. Keep the entry point small: standard library, internal files, conditional Rails hook, module config.
3. Design one clear public API first, then build internals to support it.
4. Prefer explicit modules, classes, and `define_method` over hidden magic.
5. Add Rails integration only through `ActiveSupport.on_load` and a conditional Railtie or Engine.
6. Keep the gemspec lean and move development dependencies to the Gemfile.
7. Add or update focused Minitest coverage for the public behavior you changed.

## Default Patterns

- Entry point:
  Prefer explicit `require_relative` calls and load the Railtie last with `if defined?(Rails)`.
- Configuration:
  Use module-level accessors plus lazy readers for env-backed secrets or clients.
- Errors:
  Keep a short hierarchy under `GemName::Error`. Use `ArgumentError` for invalid inputs.
- DSLs:
  Favor one top-level macro or constructor that validates keywords up front.
- Testing:
  Keep tests direct and readable. Reset shared state in `setup`.
- Documentation:
  Show the happy path first. Keep README examples short and executable.

## Use References Selectively

- Read [references/module-organization.md](references/module-organization.md) for gem layout, require order, and decomposition patterns.
- Read [references/rails-integration.md](references/rails-integration.md) for `on_load`, Railtie, Engine, generators, and optional dependency handling.
- Read [references/database-adapters.md](references/database-adapters.md) for adapter hierarchies and multi-database support.
- Read [references/testing-patterns.md](references/testing-patterns.md) for Minitest setup, gemfiles, and CI layout.
- Read [references/resources.md](references/resources.md) for canonical repositories and source files to study.

## Final Check

- The entry point stays readable without hidden control flow.
- Rails support loads lazily and conditionally.
- Runtime dependencies are justified one by one.
- Public methods and keyword options validate early.
- Tests cover the main API and at least one failure case.
