#!/usr/bin/env python3
"""Apply fork changes to an upstream release without changing the checkout."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile


# Only this fork-owned publishing workflow may be resolved automatically.
# Other conflicts, especially application code, still require manual resolution.
FORK_WORKFLOWS = frozenset({".github/workflows/docker-publish.yml"})


def git(*args, cwd=None, env=None, check=True):
    return subprocess.run(
        ["git", *args], cwd=cwd, env=env, check=check, capture_output=True
    )


def prepare_tree(upstream_tip, upstream_release, fork_ref, *, cwd=None):
    result = git(
        "merge-tree", "--write-tree", "--name-only", "-z",
        f"--merge-base={upstream_tip}", upstream_release, fork_ref,
        cwd=cwd, check=False,
    )
    if result.returncode not in (0, 1):
        raise RuntimeError(os.fsdecode(result.stderr).strip() or "git merge-tree failed")

    fields = result.stdout.split(b"\0")
    tree = fields[0].decode("ascii")
    if result.returncode == 0:
        return tree

    conflicts = []
    for field in fields[1:]:
        if not field:
            break
        conflicts.append(os.fsdecode(field))
    if not conflicts or any(path not in FORK_WORKFLOWS for path in conflicts):
        raise RuntimeError(
            "Release tree has conflicts requiring manual resolution: "
            + ", ".join(conflicts)
        )

    # Work on a separate index: neither the checkout nor its staged changes move.
    with tempfile.TemporaryDirectory(prefix="axonhub-release-index-") as directory:
        env = {**os.environ, "GIT_INDEX_FILE": str(Path(directory) / "index")}
        git("read-tree", tree, cwd=cwd, env=env)
        for path in conflicts:
            entry = git("ls-tree", "-z", fork_ref, "--", path, cwd=cwd).stdout
            if entry:
                metadata, _ = entry.rstrip(b"\0").split(b"\t", 1)
                mode, _, object_id = metadata.decode("ascii").split(" ")
                git(
                    "update-index", "--add", "--cacheinfo",
                    f"{mode},{object_id},{path}", cwd=cwd, env=env,
                )
            else:
                git("update-index", "--force-remove", "--", path, cwd=cwd, env=env)
            print(f"Preserved fork publishing workflow: {path}", file=sys.stderr)
        return git("write-tree", cwd=cwd, env=env).stdout.decode("ascii").strip()


def main():
    if len(sys.argv) != 4:
        print("Usage: prepare_fork_release_tree.py UPSTREAM_TIP UPSTREAM_RELEASE FORK_REF", file=sys.stderr)
        return 2
    try:
        print(prepare_tree(*sys.argv[1:]))
    except (RuntimeError, subprocess.CalledProcessError) as error:
        detail = os.fsdecode(error.stderr).strip() if isinstance(error, subprocess.CalledProcessError) else str(error)
        print(f"::error::{detail}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
