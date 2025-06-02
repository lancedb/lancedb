# This updates the lockfile without building
cargo metadata > /dev/null

pushd nodejs
npm install --package-lock-only
popd
pushd node
npm install --package-lock-only
popd

git commit --amend --no-edit
