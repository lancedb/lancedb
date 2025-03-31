

generate:
	openapi-generator generate \
	  -i ../rust/lancedb/resources/remote-catalog-open-api.yaml \
	  -g spring \
      -o ./catalog-adapter \
      --additional-properties=parentGroupId=com.lancedb,parentArtifactId=lancedb-parent,parentVersion=0.18.2-beta.0,parentRelativePath=../pom.xml,library=spring-boot,interfaceOnly=true,useOptional=true,openApiNullable=false,java8=true,groupId=com.lancedb,artifactId=lancedb-catalog-adapter,artifactVersion=0.18.2-beta.0,apiPackage=com.lancedb.catalog.adapter.api,modelPackage=com.lancedb.catalog.adapter.model,useTags=true,skipDefaultInterface=false,hideGenerationTimestamp=true
