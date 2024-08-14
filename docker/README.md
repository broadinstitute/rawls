### Called by GitHub Actions:
* `build_jar.sh`
  * which calls `clean_install.sh`
* `run-mysql.sh`
  * which calls `sql_validate.sh`

_See [`rawls-build-tag-publish-and-run-tests`](https://github.com/broadinstitute/rawls/blob/develop/.github/workflows/rawls-build-tag-publish-and-run-tests.yaml) and [`rawls-build`](https://github.com/broadinstitute/terra-github-workflows/blob/main/.github/workflows/rawls-build.yaml)
for GitHub Actions_


### Useful for local development:
* `run-mysql.sh`
    * which calls `sql_validate.sh`
* `jprofiler/*`

### Available for building a Docker image locally:
* `build.sh`
  * which calls `clean_install.sh`

_See instructions for local development and building Docker images in
https://github.com/broadinstitute/rawls/blob/develop/README.md_
