#!/usr/bin/env python3
"""
Publish a subset of the private BlitzTrade repo to the public blitztrade-source repo.

Usage:
    python publish_source.py                      # sync latest
    python publish_source.py --tag v2.3.0         # sync + tag with release info
    python publish_source.py --tag v2.3.0 --msg "Added bracket orders and auto-exit"

Requires: the public repo cloned at ../blitztrade-source
          (or set PUBLIC_REPO_DIR env var)
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PUBLIC_REPO = Path(
    os.environ.get("PUBLIC_REPO_DIR", SCRIPT_DIR.parent / "blitztrade-engine")
)

# ── Files to publish ──────────────────────────────────────────
# These are the files that get copied to the public repo.
# The UI (index.html, analytics.html, help.html) is NOT included.

PUBLISH_FILES = [
    "serve.py",
    "launcher.py",
    "build_app.py",
    "build_installer.py",
    "build_launcher.py",
    "publish_source.py",
    "pywebview_win32_shim.py",
    "requirements.txt",
    "requirements-build.txt",
    "start.sh",
    "start.bat",
    "watchdog.sh",
    "setup_build_env.ps1",
    "pytest.ini",
]

PUBLISH_DIRS = [
    "release_notes",
    "tests",
]

# Files that exist only in the public repo (never overwritten)
PUBLIC_ONLY = [
    "README.md",
    "PUBLISH_LOG.md",
]


def run(cmd, cwd=None, check=True):
    return subprocess.run(cmd, cwd=cwd, check=check, capture_output=True, text=True)


def get_private_commit():
    r = run(["git", "rev-parse", "HEAD"], cwd=SCRIPT_DIR)
    return r.stdout.strip()


def get_private_branch():
    r = run(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=SCRIPT_DIR)
    return r.stdout.strip()


def _truncate_subject(subject, max_len=50):
    s = (subject or "").strip()
    if len(s) <= max_len:
        return s
    if max_len <= 3:
        return s[:max_len]
    return s[: max_len - 3] + "..."


def _resolve_commit_ref(ref):
    """Resolve a commit ref/hash (short or full) to a full commit hash."""
    ref = (ref or "").strip()
    if not ref:
        return ""

    # Fast path for valid refs/unique hashes.
    r = run(["git", "rev-parse", "--verify", f"{ref}^{{commit}}"], cwd=SCRIPT_DIR, check=False)
    if r.returncode == 0:
        return r.stdout.strip()

    # Fallback: try prefix match over all commits to disambiguate historical short hashes.
    all_commits = run(["git", "rev-list", "--all"], cwd=SCRIPT_DIR, check=False)
    if all_commits.returncode != 0:
        return ""

    matches = [h.strip() for h in all_commits.stdout.splitlines() if h.strip().startswith(ref)]
    if len(matches) == 1:
        return matches[0]
    return ""


def _last_published_commit_from_log(repo):
    """Read newest published private commit hash from PUBLISH_LOG.md."""
    log_file = repo / "PUBLISH_LOG.md"
    if not log_file.exists():
        return ""
    try:
        text = log_file.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""

    marker = "**Source commit:** `"
    idx = text.find(marker)
    if idx < 0:
        return ""
    start = idx + len(marker)
    end = text.find("`", start)
    if end < 0:
        return ""
    return text[start:end].strip()


def _commit_subjects_since(last_commit, current_commit):
    """Return commit subjects in private repo from last_commit..current_commit."""
    try:
        current_resolved = _resolve_commit_ref(current_commit)
        if not current_resolved:
            return []

        last_resolved = _resolve_commit_ref(last_commit) if last_commit else ""
        if last_resolved and last_resolved != current_resolved:
            r = run(
                ["git", "log", "--pretty=%s", f"{last_resolved}..{current_resolved}"],
                cwd=SCRIPT_DIR,
                check=False,
            )
            if r.returncode == 0:
                subjects = [
                    line.strip() for line in r.stdout.splitlines() if line.strip()
                ]
                if subjects:
                    return subjects
        r = run(
            ["git", "log", "-n", "1", "--pretty=%s", current_resolved], cwd=SCRIPT_DIR
        )
        one = r.stdout.strip()
        return [one] if one else []
    except Exception:
        return []


def clean_public(repo):
    """Remove all tracked content except public-only files and .git."""
    for item in repo.iterdir():
        if item.name == ".git":
            continue
        if item.name in PUBLIC_ONLY:
            continue
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()


def copy_files(src, dst):
    for f in PUBLISH_FILES:
        s = src / f
        if s.exists():
            shutil.copy2(s, dst / f)

    for d in PUBLISH_DIRS:
        s = src / d
        t = dst / d
        if s.is_dir():
            if t.exists():
                shutil.rmtree(t)
            shutil.copytree(
                s,
                t,
                ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".pytest_cache"),
            )


def load_release_notes(tag):
    """Try to load release notes JSON for the given tag."""
    rn_file = SCRIPT_DIR / "release_notes" / f"{tag}.json"
    if rn_file.exists():
        try:
            data = json.loads(rn_file.read_text())
            return data.get("title", ""), data.get("notes", [])
        except Exception:
            pass
    return "", []


def sync_public_readme(repo):
    """If README_PUBLIC.md exists in private repo, publish it as README.md in public repo."""
    src = SCRIPT_DIR / "README_PUBLIC.md"
    dst = repo / "README.md"
    if src.exists():
        shutil.copy2(src, dst)


def append_publish_log(repo, tag, msg, commit_hash):
    """Append an entry to PUBLISH_LOG.md in the public repo."""
    log_file = repo / "PUBLISH_LOG.md"
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    last_commit = _last_published_commit_from_log(repo)
    commit_subjects = _commit_subjects_since(last_commit, commit_hash)

    if not log_file.exists():
        log_file.write_text(
            "# Publish Log\n\nHistory of source publications from the BlitzTrade private repository.\n\n---\n\n"
        )

    entry = f"### {tag or 'sync'} — {ts}\n\n"
    entry += f"- **Source commit:** `{commit_hash}` (private repo)\n"
    if msg:
        entry += f"- **Notes:** {msg}\n"
    if commit_subjects:
        entry += "- **Included commits:**\n"
        for subj in commit_subjects:
            entry += f"  - {_truncate_subject(subj)}\n"

    # If we have release notes for this tag, include them
    if tag:
        title, notes = load_release_notes(tag)
        if title:
            entry += f"- **Release:** {title}\n"
        if notes:
            for n in notes:
                entry += f"  - {n}\n"

    entry += "\n---\n\n"

    content = log_file.read_text()
    # Insert after the header section (after first ---)
    marker = "---\n\n"
    idx = content.find(marker)
    if idx >= 0:
        pos = idx + len(marker)
        content = content[:pos] + entry + content[pos:]
    else:
        content += entry

    log_file.write_text(content)


def publish(tag=None, msg=None):
    if not PUBLIC_REPO.exists() or not (PUBLIC_REPO / ".git").exists():
        print(f"ERROR: Public repo not found at {PUBLIC_REPO}")
        print(
            f"Clone it first:  git clone git@github.com:Jantoni95/blitztrade-engine.git {PUBLIC_REPO}"
        )
        sys.exit(1)

    commit_hash = get_private_commit()
    branch = get_private_branch()

    print(f"Publishing from {SCRIPT_DIR.name} ({branch} @ {commit_hash})")
    print(f"  → {PUBLIC_REPO}")

    # Pull latest public repo
    run(["git", "pull", "--rebase"], cwd=PUBLIC_REPO, check=False)

    # Clean and copy
    clean_public(PUBLIC_REPO)
    copy_files(SCRIPT_DIR, PUBLIC_REPO)
    sync_public_readme(PUBLIC_REPO)

    # Update publish log
    append_publish_log(PUBLIC_REPO, tag, msg, commit_hash)

    # Commit
    run(["git", "add", "-A"], cwd=PUBLIC_REPO)

    commit_msg = f"sync from private repo @ {commit_hash}"
    if tag:
        commit_msg = f"{tag}: {msg or 'release'} (from {commit_hash})"

    result = run(["git", "diff", "--cached", "--quiet"], cwd=PUBLIC_REPO, check=False)
    if result.returncode == 0:
        print("No changes to publish.")
        return

    run(["git", "commit", "-m", commit_msg], cwd=PUBLIC_REPO)

    # Tag if requested
    if tag:
        tag_msg = msg or f"Release {tag}"
        run(["git", "tag", "-a", tag, "-m", tag_msg], cwd=PUBLIC_REPO, check=False)

    # Push
    run(["git", "push", "--follow-tags"], cwd=PUBLIC_REPO)

    print(f"\nPublished to public repo{f' with tag {tag}' if tag else ''}.")
    print(f"  Commit: {commit_hash} → blitztrade-source")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publish source to public repo")
    parser.add_argument("--tag", help="Git tag (e.g. v2.3.0)")
    parser.add_argument("--msg", help="Release message")
    args = parser.parse_args()
    publish(args.tag, args.msg)
