

generate:
	openapi-generator generate \
      -i ./lancedb/resources/remote-catalog-open-api.yaml \
      -g rust \
      -o ./lancedb-remote-catalog-client \
      --additional-properties=packageName=lancedb-client,asyncApi=true
