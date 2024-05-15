set -e

RELEASE_TYPE=${1:-"stable"}
BUMP_MINOR=${2:-false}
TAG_PREFIX=${3:-"v"} # Such as "python-v"

readonly SELF_DIR=$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

PREV_TAG=$(git tag --sort='version:refname' | grep ^$TAG_PREFIX | python $SELF_DIR/semver_sort.py $TAG_PREFIX | tail -n 1)
echo "Found previous tag $PREV_TAG"
HEAD_COMMIT=$(git rev-parse HEAD)

# Initially, we don't want to tag if we are doing stable, because we will bump
# again later. See comment at end for why.
if [[ "$RELEASE_TYPE" == 'stable' ]]; then 
  BUMP_ARGS="--no-tag"
fi

# If last is stable and not bumping minor
if [[ $PREV_TAG != *beta* ]]; then
    if [[ "$BUMP_MINOR" != "false" ]]; then
      bump-my-version bump -vv $BUMP_ARGS minor
    else
      python $SELF_DIR/check_breaking_changes.py $PREV_TAG $HEAD_COMMIT
      bump-my-version bump -vv $BUMP_ARGS patch
    fi
else
  if [[ "$BUMP_MINOR" != "false" ]]; then
    bump-my-version bump -vv $BUMP_ARGS minor
  else
    python $SELF_DIR/check_breaking_changes.py $PREV_TAG $HEAD_COMMIT
    bump-my-version bump -vv $BUMP_ARGS pre_n
  fi
fi

# The above bump will always bump to a pre-release version. If we are releasing
# a stable version, bump the pre-release level ("pre_l") to make it stable.
if [[ $RELEASE_TYPE == 'stable' ]]; then
  bump-my-version bump -vv pre_l
fi