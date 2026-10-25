---
name: github-issue-solver
description: Solve GitHub issues end-to-end with the gh CLI, from a linked branch to an open PR that closes the issue, kept up to date after later changes. Use whenever the user references a GitHub issue (#123, issue 42) or asks to work on one.
---

# GitHub Issue Solver

Work through GitHub issues from reference to merged PR using the **`gh` CLI** for all GitHub operations. Issue references look like `#123` or "issue 42" — treat these as the same.

## Prerequisites

- **gh CLI** must be installed and authenticated (`gh auth status`). If not, tell the user to install and run `gh auth login`.
- Run commands from the repository root (the repo that contains the issue, or the repo the user is working in).

## Workflow

### 1. Resolve the issue number

From the user's message, get the issue number (e.g. `123` from `#123` or "issue 123"). If the repo isn't obvious, use the current workspace repo or ask.

### 2. Fetch issue details

Use `gh` to load the issue so you know what to implement:

```bash
gh issue view <NUMBER>
```

Use `--json title,body,labels` if you need machine-readable output. Read the title and body to understand scope, acceptance criteria, and constraints.

### 3. Create a branch for the issue

Create and checkout a branch linked to the issue so the PR will be associated correctly:

```bash
gh issue develop <NUMBER> --checkout
```

This creates a branch (e.g. `username/issue-123-description`) and checks it out. If you need a specific base branch (e.g. `main`), use `--base main`. Do **not** create branches with `git checkout -b`; use `gh issue develop` so the branch is linked to the issue.

### 4. Implement the fix

- Implement the changes required by the issue (code, tests, docs, etc.).
- Follow project conventions (see CLAUDE.md, existing code, linters).
- The implementation is finished when its tests pass and `make ci-pr` passes.

### 5. Commit, push and open the PR

As soon as the implementation is finished, publish it; the user reviews it as a PR. Make one logical commit, or a few clear ones for a large change, with a message that closes the issue:

```bash
git add -A
git commit -m "fix: <short description> (fixes #<NUMBER>)"
git push -u origin HEAD
gh pr create --title "<title>" --body-file <body.md>
```

- Write the body as CLAUDE.md's "Pull requests" section says, and start it with `Fixes #<NUMBER>`, so GitHub links the PR and closes the issue on merge.
- Add `--draft` when the user asks for a draft PR.
- With several remotes or a fork, name the target with `--repo OWNER/REPO`.

### 6. Report the PR

Give the user the PR URL and say that it closes #<NUMBER> on merge.

### 7. Update the PR after every change

When an update is finished (review feedback, a requested change, a follow-up fix): `make ci-pr` passes, commit, `git push`, then rewrite the body so it describes the PR as it now stands:

```bash
gh pr edit <PR> --body-file <body.md>
```

Then give the user the PR URL again with what changed.

## Using the gh CLI

Use **`gh`** for all GitHub interaction in this workflow:

| Task | Command |
|------|--------|
| View issue | `gh issue view <NUMBER>` |
| Create/checkout branch for issue | `gh issue develop <NUMBER> --checkout` |
| Create PR (from current branch) | `gh pr create --title "<title>" --body-file <body.md>` |
| Update PR body | `gh pr edit <PR> --body-file <body.md>` |
| Read PR review comments | `gh pr view <PR> --comments`, `gh api repos/OWNER/REPO/pulls/<PR>/comments` |
| Check auth | `gh auth status` |
| Repo context | `gh repo view` or `-R OWNER/REPO` on any command |

Do not use raw GitHub API calls or manual `git remote`/PR creation when these commands suffice.

## Edge cases

- **Issue in another repo:** Use `-R OWNER/REPO` with `gh issue view` and `gh issue develop`. Push and `gh pr create` from a clone of that repo (or the fork you're using).
- **Branch already exists:** If `gh issue develop` says the branch exists, list with `gh issue develop --list <NUMBER>`, then `git fetch` and `git checkout <branch-name>`.
