# Cursor Workspace Safety Rules

## General
- Do NOT run destructive shell commands.
- NEVER delete, move, or overwrite files without explicit confirmation.
- If an action may modify the filesystem, ask before executing.

## Forbidden Commands
The following commands are strictly forbidden:
- Remove-Item, rm, del, rmdir
- shutil.rmtree
- git clean -fd
- git reset --hard
- git push --force
- Move-Item, Copy-Item, Rename-Item (unless explicitly approved)

## Allowed Commands
Allowed without confirmation:
- dir, tree, Get-ChildItem
- git status, git diff, git log
- python -c (read-only)
- pip list

## Python Environment
- Always use a virtual environment (.venv).
- Never install packages globally.
- If no .venv exists, ask to create one.

## Git Workflow
- Prefer Git UI over terminal commands.
- Never rewrite Git history.
- Avoid rebase and force operations.

## Filesystem Scope
- Operate ONLY within the project directory.
- Never access system or user home directories.

## Default Behavior
- If unsure, STOP and ask.
