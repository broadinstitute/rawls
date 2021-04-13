Quickstart: running integration tests locally on Mac/Docker 

## Running in docker

See [firecloud-automated-testing](https://github.com/broadinstitute/firecloud-automated-testing).


## Running directly (with real chrome)

### Set Up

```
brew install chromedriver
```

Note: Leonardo integration tests are not currently web-based but may fail due to dependencies without chromedriver

Render configs:
```bash
./render-local-env.sh [branch of firecloud-automated-testing] [vault token] [env] [service root]
```

**Arguments:** (arguments are positional)

* branch of firecloud-automated-testing
    * Configs branch; defaults to `master`
* Vault auth token
	* Defaults to reading it from the .vault-token via `$(cat ~/.vault-token)`.
* env
	* Environment of your FiaB; defaults to `dev`
* service root
    * the name of your local clone of rawls if not `rawls`
	
##### Using a local UI

Set `LOCAL_UI=true` before calling `render-local-env.sh`.   When starting your UI, run:

```bash
FIAB=true ./config/docker-rsync-local-ui.sh
```
	
### Run tests

#### From IntelliJ

In IntelliJ, go to `Run` > `Edit Configurations...`, select `ScalaTest` under `Defaults`, and make sure there is a `Build` task configured to run before launch.

Now, simply open the test spec, right-click on the class name or a specific test string, and select `Run` or `Debug` as needed. A good one to start with is `GoogleSpec` to make sure your base configuration is correct. All test code lives in `automation/src/test/scala`. FireCloud test suites can be found in `automation/src/test/scala/org/broadinstitute/dsde/firecloud/test`.

#### From the command line

To run all tests:

```bash
sbt test
```

To run a single suite:

```bash
sbt "testOnly *GoogleSpec"
```

To run a single test within a suite:

```bash
# matches test via substring
sbt "testOnly *GoogleSpec -- -z \"have a search field\""
```

For more information see [SBT's documentation](http://www.scala-sbt.org/0.13/docs/Testing.html#Test+Framework+Arguments).


### Hotswapping Rawls jar in your fiab

To avoid the lengthy cycle of updating your fiab via fiab-stop and fiab-start, 
you can instead run the [automation/hotswap.sh](hotswap.sh) script from the repo's root folder. 

### Troubleshooting

If you see an error like this:
* ```javax.net.ssl.SSLHandshakeException: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target```
  * Then check to make sure that your test config does not include `enableSNIExtension`. This flag is no longer needed.
