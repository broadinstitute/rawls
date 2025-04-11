# Quicksilver Data Tables Performance Testing

## Quick Start

* install k6: `brew install k6`
* edit `quicksilver.env` as appropriate for your test
* `source quicksilver.env`
* `gcloud auth login` as a user with access to the workspaces under test
* `QUICKSILVER_USER_TOKEN=$(gcloud auth print-access-token) k6 run --summary-mode=full quicksilver.js`

## k6

These performance tests use the `k6` framework. For details on installing k6,
see https://grafana.com/docs/k6/latest/set-up/install-k6/.

Once installed, execute `k6 run --help` for options on running tests.

## Interpreting Results

The `--summary-mode=full` output from the k6 test run shows timing per _scenario_. You can view the
scenario definitions inside `quicksilver.js`; they are named intuitively. Compare the timings for
the "test" and "baseline" scenarios to look for any notable differences.

_example output:_
![example-k6-output.png](example-k6-output.png)

## Testing Against a Different Environment

To test against a different instance of Rawls, such as a locally-running Rawls, change the value of the
`QUICKSILVER_TERRA_INSTANCE` environment variable. See [quicksilver.env](quicksilver.env) for syntax.

To test against different workspaces, change the value of the
`QUICKSILVER_WS_PREFIX` environment variable. See [quicksilver.env](quicksilver.env) for syntax.
*IMPORTANT:* the performance tests assume that three entities of type `target` with names `one`, `two`,
and `three` already exist in the workspaces under test. Without those entities, tests will fail.
