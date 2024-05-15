"""
Check whether there are any breaking changes in the PRs between the base and head commits.
If there are, assert that we have incremented the minor version.
"""
import argparse
import os
from packaging.version import parse

from github import Github

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("base")
    parser.add_argument("head")
    parser.add_argument("last_stable_version")
    parser.add_argument("current_version")
    args = parser.parse_args()

    repo = Github(os.environ["GITHUB_TOKEN"]).get_repo(os.environ["GITHUB_REPOSITORY"])
    commits = repo.compare(args.base, args.head).commits
    prs = (pr for commit in commits for pr in commit.get_pulls())

    for pr in prs:
        if any(label.name == "breaking-change" for label in pr.labels):
            print(f"Breaking change in PR: {pr.html_url}")
            break
    else:
        print("No breaking changes found.")
        exit(0)
    
    last_stable_version = parse(args.last_stable_version)
    current_version = parse(args.current_version)
    if current_version.minor <= last_stable_version.minor:
        print("Minor version is not greater than the last stable version.")
        exit(1)
