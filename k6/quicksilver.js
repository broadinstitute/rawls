import http from 'k6/http';
import { group, sleep, check } from 'k6';

export const options = {
  // define each test scenario to be run
  scenarios: {
    // entityQuery baseline and test run in parallel; each has three virtual users and makes as many requests as possible
    // within 20 seconds.
    entityQueryBaseline: {
      exec: 'entityQuery',
      tags: { rawlsApi: 'entityQuery' },
      env: { TEST_GROUP: 'baseline' },
      executor: 'constant-vus',
      vus: 3,
      duration: '20s'
    },
    entityQueryTest: {
      exec: 'entityQuery',
      tags: { rawlsApi: 'entityQuery' },
      env: { TEST_GROUP: 'test' },
      executor: 'constant-vus',
      vus: 3,
      duration: '20s'
    },
    // getEntity baseline and test run in parallel; each has three virtual users and makes as many requests as possible
    // within 20 seconds. The getEntity scenarios start after the entityQuery scenarios finish, by specifying startTime.
    getEntityBaseline: {
      exec: 'getEntity',
      tags: { rawlsApi: 'getEntity' },
      env: { TEST_GROUP: 'baseline' },
      executor: 'constant-vus',
      vus: 3,
      duration: '20s',
      startTime: '22s'
    },
    getEntityTest: {
      exec: 'getEntity',
      tags: { rawlsApi: 'getEntity' },
      env: { TEST_GROUP: 'test' },
      executor: 'constant-vus',
      vus: 3,
      duration: '20s',
      startTime: '22s',
    },
    // putEntity baseline and test run in parallel; each makes a total of 20 requests, using 2 virtual users.
    // The putEntity scenarios start after the getEntity scenarios finish, by specifying startTime.
    putEntityBaseline: {
      exec: 'putEntity',
      tags: { rawlsApi: 'putEntity' },
      env: { TEST_GROUP: 'baseline' },
      executor: 'shared-iterations',
      vus: 2,
      iterations: 20,
      startTime: '44s'
    },
    putEntityTest: {
      exec: 'putEntity',
      tags: { rawlsApi: 'putEntity' },
      env: { TEST_GROUP: 'test' },
      executor: 'shared-iterations',
      vus: 2,
      iterations: 20,
      startTime: '44s',
    },
  }
}

// paginated search; this is the API that populates data tables in the UI
export function entityQuery() {
  group(`${__ENV.TEST_GROUP}`, function() {
    let res = http.get(
      `${workspaceRoot(__ENV.TEST_GROUP)}/entityQuery/files?page=1&pageSize=10&sortField=name&sortDirection=asc&filterOperator=and`,
      defaultParams);
    check(res, { "status is 200": (res) => res.status === 200 });
    sleep(.1);
  });
}

// get a single entity
export function getEntity() {
  group(`${__ENV.TEST_GROUP}`, function() {
    let res = http.get(
      `${workspaceRoot(__ENV.TEST_GROUP)}/entities/files/files.0e0f2af4-8406-5d43-950c-6ad9a7d999fb.1`,
      defaultParams);
    check(res, { "status is 200": (res) => res.status === 200 });
    sleep(.1);
  });
}

// put a single entity
export function putEntity() {
  group(`${__ENV.TEST_GROUP}`, function() {
    const name = `id-${Math.random()}`
    const entityType = "loadTestPuts"
    const entity = {
      name,
      entityType,
      attributes: {
        copyOfName: name,
        randomNumber: Math.random()*10000,
        ref1: {
          entityType: 'files',
          entityName: 'files.0e0f2af4-8406-5d43-950c-6ad9a7d999fb.1'
        },
        ref2: {
          entityType: 'files',
          entityName: 'files.072a18ca-239b-570b-8509-73878b87988d.1'
        }
      }
    }

    let res = http.post(
      `${workspaceRoot(__ENV.TEST_GROUP)}/entities`,
      JSON.stringify(entity),
      defaultParams);
    check(res, { "status is 201": (res) => res.status === 201 });
    sleep(.1);
  });
}

export function setup() {
  // setup code, such as inserting test data before the test scenarios run
}

export function teardown(data) {
  // teardown code, such as deleting data created by test scenarios
}

// helper functions
const defaultHeaders = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${__ENV.QUICKSILVER_USER_TOKEN}`
  };

const defaultParams = {headers: defaultHeaders}

const workspaceRoot = (testGroup) => {
  return `${__ENV.QUICKSILVER_TERRA_INSTANCE}/api/workspaces/${__ENV.QUICKSILVER_WS_PREFIX}${testGroup}`;
}

