Running the `publishRelease.sh` script publishes a release of rawls-model, workbench-util and workbench-google to Artifactory. You should do this manually from the base directory of the repo when you change something in `model/src`, `util/src` or `google/src` so clients such as Orchestration can bump their version and stay in sync. In Orchestration, the file that contains the reference is project/Dependencies.scala.

Jenkins runs `publishSnapshot.sh` on every dev build, but that makes "unofficial" `-SNAP` versions.

Note that you need `ARTIFACTORY_USERNAME` and `ARTIFACTORY_PASSWORD` in your env for either of these to work (you can get this info from jenkins config for rawls).

cmd$ ARTIFACTORY_USERNAME=<name> ARTIFACTORY_PASSWORD=<pw> core/src/bin/publishRelease.sh

You can view what is in the artifactory here: http://artifactory.broadinstitute.org/artifactory/libs-release-local
