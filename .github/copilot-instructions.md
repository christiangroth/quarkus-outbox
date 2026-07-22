# Quarkus Outbox

## Build & Test Commands

```bash
# Run full build (includes tests and static analysis)
./gradlew build

# Run tests only
./gradlew test

```

## Green Build Requirement

CI (`gradle.yml`) runs automatically on every push to every branch (no `branches: [main]` filter), so pushes are validated without waiting for a PR. Still:

- **Run `./gradlew build` locally before pushing** to catch issues early.
- **Never use `[no ci]`/`[skip ci]`** in commit messages to bypass this validation.
- If CI fails, fix the underlying issue — do not skip it.

## Formatting

All code must follow the formatting rules in `.editorconfig`. The most important rules for Kotlin:

- **2-space indentation** (not 4), no tabs
- **CRLF line endings**
- **Max line length:** 180 characters
- **Insert final newline** in every file

Always format new and edited files according to `.editorconfig` before committing.

## Documentation

- **Architecture:** [docs/arc42.md](docs/arc42.md)

## Release Note Snippets

**Snippet filename:** `docs/releasenotes/snippets/{branch-last-segment}-{type}.md` where `{type}` is one of `bugfix` or `feature`.

**Snippet content:** Briefly describe what was changed or added on the branch. Each line should follow the pattern `* {branch-last-segment}: Description of the change.` Feel free to use multiple short lines, describing the change without technical detail. Only include **user-facing or dependency changes** in release notes. Do not add implementation details, refactoring notes, or internal structural changes (e.g. package renames, build task additions).

**Type selection:** Use `feature` for new user-facing functionality. Use `bugfix` for fixes and chore/internal changes (e.g. refactoring, configuration restructuring, dependency updates).
