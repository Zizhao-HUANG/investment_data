#!/usr/bin/env bash
set -euo pipefail

REPO_PATH="/home/shenzi/qlib_selfhost/investment_data"

cd "$REPO_PATH"

# 显示当前仓库信息
echo "[INFO] Repository: $(pwd)"
echo "[INFO] Remote(s):"
git remote -v | cat

# 获取当前分支
current_branch="$(git rev-parse --abbrev-ref HEAD)"
echo "[INFO] Current branch: ${current_branch}"

echo "[INFO] Fetching all remotes and tags..."
git fetch --all --tags | cat

# 确保在当前分支
git checkout "$current_branch" | cat

echo "[INFO] Pulling from origin/${current_branch} with merge (no-edit)..."
if git pull --no-rebase --no-edit --tags origin "$current_branch" | cat; then
  echo "[INFO] Merge pull succeeded."
else
  echo "[WARN] Merge pull failed. Trying to abort merge and rebase instead..."
  # 尝试中止合并（若无合并在进行会失败，忽略）
  git merge --abort 2>/dev/null || true
  echo "[INFO] Pulling with rebase..."
  if git pull --rebase --tags origin "$current_branch" | cat; then
    echo "[INFO] Rebase pull succeeded."
  else
    echo "[ERROR] 自动合并与rebase均失败，请手动解决冲突后重试。" >&2
    exit 1
  fi
fi

echo "[INFO] Pushing branch and tags to origin..."
git push origin "$current_branch" | cat
git push --tags origin | cat

echo "[INFO] Done."


