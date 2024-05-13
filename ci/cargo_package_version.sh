cargo metadata --no-deps --format-version 1 \
    | jq ".packages[] | select(.name == \"${1}\") | .version" \
    | xargs echo
