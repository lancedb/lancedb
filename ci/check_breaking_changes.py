"""
Assert there were no breaking changes between the two git revisions.
"""
import argparse
import os

from github import Github

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("base")
    parser.add_argument("head")
    args = parser.parse_args()

    repo = Github().get_repo(os.environ["GITHUB_REPOSITORY"])
    breakpoint()
    commits = repo.compare(args.base, args.head).commits
    prs = (pr for commit in commits for pr in commit.get_pulls())

    for pr in prs:
        if any(label.name == "breaking-change" for label in pr.labels):
            raise ValueError(f"Breaking change in PR: {pr.html_url}")
    
    print("No breaking changes found.")
