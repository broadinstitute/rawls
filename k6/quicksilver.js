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
    // getEntityMetadata baseline and test run in parallel; each has three virtual users and makes as many requests as possible
    // within 20 seconds. The getEntityMetadata scenarios start after the getEntity scenarios finish, by specifying startTime.
    getEntityMetadataBaseline: {
      exec: 'getEntityMetadata',
      tags: { rawlsApi: 'getEntityMetadata' },
      env: { TEST_GROUP: 'baseline, cached' },
      executor: 'constant-vus',
      vus: 3,
      duration: '20s',
      startTime: '44s'
    },
    getEntityMetadataBaselineUncached: {
      exec: 'getEntityMetadataUncached',
      tags: { rawlsApi: 'getEntityMetadata' },
      env: { TEST_GROUP: 'baseline, uncached' },
      executor: 'constant-vus',
      vus: 3,
      duration: '20s',
      startTime: '44s'
    },
    getEntityMetadataTest: {
      exec: 'getEntityMetadata',
      tags: { rawlsApi: 'getEntityMetadata' },
      env: { TEST_GROUP: 'test' },
      executor: 'constant-vus',
      vus: 3,
      duration: '20s',
      startTime: '44s',
    },
    // putEntity baseline and test run in parallel; each makes a total of 20 requests, using 2 virtual users.
    // The putEntity scenarios start after the getEntityMetadata scenarios finish, by specifying startTime.
    putEntityBaseline: {
      exec: 'putEntity',
      tags: { rawlsApi: 'putEntity' },
      env: { TEST_GROUP: 'baseline' },
      executor: 'shared-iterations',
      vus: 2,
      iterations: 20,
      startTime: '66s'
    },
    putEntityTest: {
      exec: 'putEntity',
      tags: { rawlsApi: 'putEntity' },
      env: { TEST_GROUP: 'test' },
      executor: 'shared-iterations',
      vus: 2,
      iterations: 20,
      startTime: '66s',
    },
  }
}

// paginated search; this is the API that populates data tables in the UI
export function entityQuery() {
  group(`${__ENV.TEST_GROUP}`, function() {
    // page size 100 replicates what Terra UI asks for
    let res = http.get(
      `${workspaceRoot(__ENV.TEST_GROUP)}/entityQuery/file_inventory?page=1&pageSize=100&sortField=name&sortDirection=asc&filterOperator=and`,
      defaultParams);
    check(res, { "status is 200": (res) => res.status === 200 });
    sleep(.1);
  });
}

// get a single entity
export function getEntity() {
  group(`${__ENV.TEST_GROUP}`, function() {
    let res = http.get(
      `${workspaceRoot(__ENV.TEST_GROUP)}/entities/target/one`,
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
        ref: {
          entityType: 'target',
          entityName: 'one'
        },
        refList: {
          itemsType: 'EntityReference',
          items: [
            {
              entityType: 'target',
              entityName: 'two'
            }, {
              entityType: 'target',
              entityName: 'three'
            }
          ]
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

// get entity type metadata
export function getEntityMetadata() {
  metadataTest(true);
}
export function getEntityMetadataUncached() {
  metadataTest(false);
}

function metadataTest(useCache) {
  group(`${__ENV.TEST_GROUP}`, function() {
    let res = http.get(
        `${workspaceRoot(__ENV.TEST_GROUP)}/entities?useCache=${useCache}`,
        defaultParams);
    check(res, { "status is 200": (res) => res.status === 200 });
    sleep(.1);
  });
}

// delete all entities of a given type
export function deleteTable(testGroup, tableName) {
  let res = http.del(
      `${workspaceRoot(testGroup)}/entityTypes/${tableName}`, null, defaultParams);
  return res.status;
}

export function setup() {
  // setup code, such as inserting test data before the test scenarios run
}

// teardown code, runs after all test scenarios are done
export function teardown(data) {

  const testGroups = ['baseline', 'test'];
  const tablesToDelete = ['loadTestPuts'];

  for (const testGroup of testGroups) {
    for (const table of tablesToDelete) {
      console.log(`deleting table '${table}' for test group '${testGroup}' ...`)
      const statusCode = deleteTable(testGroup, table);
      console.log(`... deleteTable ${testGroup}/${table} result is ${statusCode}`)
    }
  }
}

// helper functions
const defaultHeaders = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${__ENV.QUICKSILVER_USER_TOKEN}`
  };

const defaultParams = {headers: defaultHeaders}

const workspaceRoot = (testGroup) => {
  // take everything up to the first comma, allowing for "annotations" to the testGroup
  const groupBase = testGroup.split(",")[0];
  return `${__ENV.QUICKSILVER_TERRA_INSTANCE}/api/workspaces/${__ENV.QUICKSILVER_WS_PREFIX}${groupBase}`;
}

