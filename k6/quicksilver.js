import http from 'k6/http';
import { group, sleep, check } from 'k6';

export const options = {
  // define each test scenario to be run
  scenarios: {
    // entityQuery baseline and test run in parallel; each has three virtual users and makes as many requests as possible
    // within 20 seconds.
    entityQueryBaseline: {
      exec: 'entityQuery',
      tags: { rawlsApi: 'entityQuery', feature: 'sorting' },
      env: { TEST_GROUP: 'baseline' },
      executor: 'constant-vus',
      vus: 3,
      duration: '20s'
    },
    entityQueryTest: {
      exec: 'entityQuery',
      tags: { rawlsApi: 'entityQuery', feature: 'sorting' },
      env: { TEST_GROUP: 'test' },
      executor: 'constant-vus',
      vus: 3,
      duration: '20s'
    },
    // entityQuery filter-by-column tests.
    entityQueryFilterByColumnBaseline: {
      exec: 'entityQueryFilterByColumn',
      tags: { rawlsApi: 'entityQuery', feature: 'filter-by-column'  },
      env: { TEST_GROUP: 'baseline' },
      executor: 'constant-vus',
      vus: 3,
      duration: '20s',
      startTime: '22s'
    },
    entityQueryFilterByColumnTest: {
      exec: 'entityQueryFilterByColumn',
      tags: { rawlsApi: 'entityQuery', feature: 'filter-by-column' },
      env: { TEST_GROUP: 'test' },
      executor: 'constant-vus',
      vus: 3,
      duration: '20s',
      startTime: '22s'
    },
    // entityQuery all-column search
    entityQueryAllColumnSearchBaseline: {
      exec: 'entityQuery',
      tags: { rawlsApi: 'entityQuery', feature: 'search' },
      env: { TEST_GROUP: 'baseline' },
      executor: 'constant-vus',
      vus: 3,
      duration: '20s',
      startTime: '44s'
    },
    entityQueryAllColumnSearchTest: {
      exec: 'entityQuery',
      tags: { rawlsApi: 'entityQuery', feature: 'search' },
      env: { TEST_GROUP: 'test' },
      executor: 'constant-vus',
      vus: 3,
      duration: '20s',
      startTime: '44s'
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
      startTime: '66s'
    },
    getEntityTest: {
      exec: 'getEntity',
      tags: { rawlsApi: 'getEntity' },
      env: { TEST_GROUP: 'test' },
      executor: 'constant-vus',
      vus: 3,
      duration: '20s',
      startTime: '66s',
    },
    // getEntityMetadata baseline and test run in parallel; each has three virtual users and makes as many requests as possible
    // within 20 seconds. The getEntityMetadata scenarios start after the getEntity scenarios finish, by specifying startTime.
    getEntityMetadataBaseline: {
      exec: 'getEntityMetadata',
      tags: { rawlsApi: 'getEntityMetadata', feature: 'cached' },
      env: { TEST_GROUP: 'baseline' },
      executor: 'constant-vus',
      vus: 3,
      duration: '20s',
      startTime: '88s'
    },
    getEntityMetadataBaselineUncached: {
      exec: 'getEntityMetadataUncached',
      tags: { rawlsApi: 'getEntityMetadata', feature: 'uncached' },
      env: { TEST_GROUP: 'baseline' },
      executor: 'constant-vus',
      vus: 3,
      duration: '20s',
      startTime: '88s'
    },
    getEntityMetadataTest: {
      exec: 'getEntityMetadata',
      tags: { rawlsApi: 'getEntityMetadata', feature: 'uncached' },
      env: { TEST_GROUP: 'test' },
      executor: 'constant-vus',
      vus: 3,
      duration: '20s',
      startTime: '88s',
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
      startTime: '110s'
    },
    putEntityTest: {
      exec: 'putEntity',
      tags: { rawlsApi: 'putEntity' },
      env: { TEST_GROUP: 'test' },
      executor: 'shared-iterations',
      vus: 2,
      iterations: 20,
      startTime: '110s',
    },
  }
}

// paginated search; this is the API that populates data tables in the UI
export function entityQuery() {
  // custom sort column; no search or filter
  entityQueryImpl('anvil_activity', 'page=1&pageSize=100&sortField=activity_id&sortDirection=asc&filterOperator=and');
}

export function entityQueryFilterByColumn() {
  // filter on the `activity_type` column for the value `Unknown`; sort by name
  entityQueryImpl('anvil_activity', 'columnFilter=activity_type%3DUnknown&page=1&pageSize=100&sortField=name&sortDirection=asc&filterOperator=and');
}

export function entityQueryAllColumnSearch() {
  // search across all columns for the value `Unknown`; sort by name
  entityQueryImpl('anvil_activity', 'filterTerms=Unknown&page=1&pageSize=100&sortField=name&sortDirection=asc&filterOperator=and');
}

function entityQueryImpl(entityType, queryString) {
  group(`${__ENV.TEST_GROUP}`, function() {
    let res = http.get(
        `${workspaceRoot(__ENV.TEST_GROUP)}/entityQuery/${entityType}?${queryString}`,
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

