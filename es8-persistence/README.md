# ES8 Persistence

This module provides ES8 persistence when indexing workflows and tasks.

### ES Breaking changes

From ES7 to ES8 there are additional breaking changes which affect the ES8-persistence module implementation.
* Mapping type deprecation continues
* Templates API changes
* TransportClient is removed
* Security and authentication changes
* Removal of deprecated APIs

More information can be found here: https://www.elastic.co/guide/en/elasticsearch/reference/8.0/breaking-changes-8.0.html

## Build

1. In order to use ES8, you must change the following files from ES7 to ES8:

https://github.com/conductor-oss/conductor/blob/main/build.gradle
https://github.com/conductor-oss/conductor/blob/main/server/src/main/resources/application.properties

In file:
- /build.gradle
change ext['elasticsearch.version'] from revElasticSearch7 to revElasticSearch8

In file:
- /server/src/main/resources/application.properties
change conductor.elasticsearch.version from 7 to 8

Also you need to recreate dependencies.lock files with ES8 dependencies. To do that delete all dependencies.lock files and then run:

```
./gradlew generateLock updateLock saveLock
```

2. To use ES8 for all modules including test-harness, you must also change the following files:

https://github.com/conductor-oss/conductor/blob/main/test-harness/build.gradle
https://github.com/conductor-oss/conductor/blob/main/test-harness/src/test/java/com/netflix/conductor/test/integration/AbstractEndToEndTest.java

In file:
- /test-harness/build.gradle

Update Elasticsearch dependencies to version 8.

