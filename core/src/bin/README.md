Running the `publishRelease.sh` script publishes a release of rawls-model, workbench-util and workbench-google to Artifactory. You should do this manually when you change something in `model/src`, `util/src` or `google/src` so clients such as Orchestration can bump their version and stay in sync.

Jenkins runs `publishSnapshot.sh` on every dev build, but that makes "unofficial" `-SNAP` versions.

Note that you need `ARTIFACTORY_USERNAME` and `ARTIFACTORY_PASSWORD` in your env for either of these to work.