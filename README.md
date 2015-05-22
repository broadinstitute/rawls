[![Build Status](https://travis-ci.org/broadinstitute/rawls.svg?branch=master)](https://travis-ci.org/broadinstitute/rawls)

[![Coverage Status](https://coveralls.io/repos/broadinstitute/rawls/badge.svg)](https://coveralls.io/r/broadinstitute/rawls)

DSDE Workspace Service

Current workspace json looks like
```json
  {
    "name": "foo",
    "createdDate": "yyyy-MM-dd'T'HH:mm:ssZZ",
    "entities": {
      "individuals": {
        "individual id": {
          "name": "individual name",
          "attributes": {
            "key": "SOME_VALUE",
            "samples": [
              { "entityType": "samples", "entityName": "sample id" }
            ]
          }
        }
      },
      "samples": {
        "sample id": {
          "name": "sample name",
          "attributes": {
            "key": "SOME_VALUE",
          }
        }
      }
    }
  }
```
