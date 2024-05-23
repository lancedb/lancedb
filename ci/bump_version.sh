set -e

RELEASE_TYPE=${1:-"stable"}
BUMP_MINOR=${2:-false}
TAG_PREFIX=${3:-"v"} # Such as "python-v"
HEAD_SHA=${4:-$(git rev-parse HEAD)}

readonly SELF_DIR=$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

PREV_TAG=$(git tag --sort='version:refname' | grep ^$TAG_PREFIX | python $SELF_DIR/semver_sort.py $TAG_PREFIX | tail -n 1)
echo "Found previous tag $PREV_TAG"

# Initially, we don't want to tag if we are doing stable, because we will bump
# again later. See comment at end for why.
if [[ "$RELEASE_TYPE" == 'stable' ]]; then 
  BUMP_ARGS="--no-tag"
fi

# If last is stable and not bumping minor
if [[ $PREV_TAG != *beta* ]]; then
    if [[ "$BUMP_MINOR" != "false" ]]; then
      # X.Y.Z -> X.(Y+1).0-beta.0
      bump-my-version bump -vv $BUMP_ARGS minor
    else
      # X.Y.Z -> X.Y.(Z+1)-beta.0
      bump-my-version bump -vv $BUMP_ARGS patch
    fi
else
  if [[ "$BUMP_MINOR" != "false" ]]; then
    # X.Y.Z-beta.N -> X.(Y+1).0-beta.0
    bump-my-version bump -vv $BUMP_ARGS minor
  else
    # X.Y.Z-beta.N -> X.Y.Z-beta.(N+1)
    bump-my-version bump -vv $BUMP_ARGS pre_n
  fi
fi

# The above bump will always bump to a pre-release version. If we are releasing
# a stable version, bump the pre-release level ("pre_l") to make it stable.
if [[ $RELEASE_TYPE == 'stable' ]]; then
  # X.Y.Z-beta.N -> X.Y.Z
  bump-my-version bump -vv pre_l
fi

# Validate that we have incremented version appropriately for breaking changes
NEW_TAG=$(git describe --tags --exact-match HEAD)
NEW_VERSION=$(echo $NEW_TAG | sed "s/^$TAG_PREFIX//")
LAST_STABLE_RELEASE=$(git tag --sort='version:refname' | grep ^$TAG_PREFIX | grep -v beta | grep -vF "$NEW_TAG" | python $SELF_DIR/semver_sort.py $TAG_PREFIX | tail -n 1)
LAST_STABLE_VERSION=$(echo $LAST_STABLE_RELEASE | sed "s/^$TAG_PREFIX//")

python $SELF_DIR/check_breaking_changes.py $LAST_STABLE_RELEASE $HEAD_SHA $LAST_STABLE_VERSION $NEW_VERSION
