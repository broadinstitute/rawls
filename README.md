DSDE Workspace Service

Current workspace json looks like

  {
    "name": "foo",
    "createdDate": "yyyy-MM-dd'T'HH:mm:ssZZ",
    "entities": {
      "individuals": {
        "indiv id": {
          "name": "indiv name",
          "attributes": {
            "key": value,
            "samples": [
              { "entityType": "samples", "entityName": "samp id" }
            ]
          }
        }
      }
      "samples": {
        "samp id": {
          "name": "sampe name",
          "attributes": {
            "key": value,
          }
        }
      }
    }
  }
