#!/bin/bash
set -euo pipefail

TARGET_BRANCH="codex/punchlist-v1"
SOURCE_BRANCH="main"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Not inside a git repository."
  exit 1
fi

CURRENT_BRANCH="$(git branch --show-current)"

if [[ -n "$(git status --porcelain)" ]]; then
  echo "Working tree is not clean. Commit, stash, or discard changes first."
  exit 1
fi

if ! git show-ref --verify --quiet "refs/heads/$SOURCE_BRANCH"; then
  echo "Local source branch '$SOURCE_BRANCH' not found."
  exit 1
fi

if ! git show-ref --verify --quiet "refs/heads/$TARGET_BRANCH"; then
  echo "Local target branch '$TARGET_BRANCH' not found."
  exit 1
fi

echo "Switching to $TARGET_BRANCH"
git checkout "$TARGET_BRANCH"

echo "Merging $SOURCE_BRANCH into $TARGET_BRANCH"
git merge "$SOURCE_BRANCH"

echo "Merge complete."
echo "Current branch: $(git branch --show-current)"
echo "If desired, push with: git push --set-upstream origin $TARGET_BRANCH"

if [[ "$CURRENT_BRANCH" != "$TARGET_BRANCH" ]]; then
  echo "Original branch was '$CURRENT_BRANCH'."
fi
