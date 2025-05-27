import http from 'k6/http';
import { group, sleep, check } from 'k6';
import { generateBatchUpsert } from './batchOperations.js';

/**********************************************************************
 * Test scenarios
 **********************************************************************/

export const options = {
  // define each test scenario to be run
  // for info on the various executor options (VUs, duration, iterations, etc) see
  // https://grafana.com/docs/k6/latest/using-k6/scenarios/executors/
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
    // the putEntity scenarios require < 5s to complete
    putEntityBaseline: {
      exec: 'putEntity',
      tags: { rawlsApi: 'putEntity' },
      env: { TEST_GROUP: 'baseline' },
      executor: 'shared-iterations',
      vus: 2,
      iterations: 20,
      startTime: '110s',
      maxDuration: '10s'
    },
    putEntityTest: {
      exec: 'putEntity',
      tags: { rawlsApi: 'putEntity' },
      env: { TEST_GROUP: 'test' },
      executor: 'shared-iterations',
      vus: 2,
      iterations: 20,
      startTime: '110s',
      maxDuration: '10s'
    },
    // batchUpserts. These use 1 VU to serialize requests to avoid lock contention.
    // run these tests last; they are the longest to execute.
    batchUpsertBaseline: {
      exec: 'batchUpsertAndDelete',
      env: { TEST_GROUP: 'baseline' },
      executor: 'shared-iterations',
      vus: 1,
      iterations: 10,
      startTime: '120s',
      maxDuration: '15m' // baseline test may not complete in this time; we cut it off anyway
    },
    batchUpsertTest: {
      exec: 'batchUpsertAndDelete',
      env: { TEST_GROUP: 'test' },
      executor: 'shared-iterations',
      vus: 1,
      iterations: 10,
      startTime: '120s',
      maxDuration: '15m'
    },
  }
}

/**********************************************************************
 * entityQuery
 **********************************************************************/

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

/**********************************************************************
 * get/put single entity
 **********************************************************************/

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

/**********************************************************************
 * batchUpsert and delete
 **********************************************************************/

/**
 * Tests batchUpsert from empty, batchUpsert to modify existing entities,
 * delete-by-pointer, and delete-by-type all in one go.
 * 
 * Since we want to test batchUpsert from empty, we need to constantly be deleting entities,
 * otherwise we'd always be testing updates, not inserts. And since we want to test deletes,
 * we always need entities to delete. So these go together well.
 */
export function batchUpsertAndDelete() {
  group(`${__ENV.TEST_GROUP}`, function() {
    const testType = "batchUpsertTest";
    const smallUpsertSize = 250;
    const largeUpsertSize = 4000;

    // note that each request below sends a separate set of tags

    // generate a small batchUpsert payload and insert it
    const smallInitialUpsert = generateBatchUpsert(testType, smallUpsertSize);
    const res1 = http.post(
      `${workspaceRoot(__ENV.TEST_GROUP)}/entities/batchUpsert`,
      JSON.stringify(smallInitialUpsert),
      {headers: defaultHeaders, tags: { rawlsApi: 'batchUpsert', feature: 'insert', size: smallUpsertSize }});
    check(res1, { "status is 204": (res) => res.status === 204 });
    sleep(.1);

    // re-generate the small batchUpsert payload to get some changes, and update it
    const smallUpdate = generateBatchUpsert(testType, smallUpsertSize);
    const res2 = http.post(
      `${workspaceRoot(__ENV.TEST_GROUP)}/entities/batchUpsert`,
      JSON.stringify(smallUpdate),
      {headers: defaultHeaders, tags: { rawlsApi: 'batchUpsert', feature: 'update', size: smallUpsertSize }});
    check(res2, { "status is 204": (res) => res.status === 204 });
    sleep(.1);

    // translate the smallUpdate into {entityType, entityName} pairs
    // for deleteByPointer
    const deleteByPointer = smallUpdate.map((entity) => {
      return {
        entityType: entity.entityType,
        entityName: entity.name
      }
    });
    const res3 = http.post(
      `${workspaceRoot(__ENV.TEST_GROUP)}/entities/delete`,
      JSON.stringify(deleteByPointer),
      {headers: defaultHeaders, tags: { rawlsApi: 'deleteEntities', size: smallUpsertSize }});
    check(res3, { "status is 204": (res) => res.status === 204 });
    sleep(.1);

    // generate a small batchUpsert payload and insert it
    const largeInitialUpsert = generateBatchUpsert(testType, largeUpsertSize);
    const res4 = http.post(
      `${workspaceRoot(__ENV.TEST_GROUP)}/entities/batchUpsert`,
      JSON.stringify(largeInitialUpsert),
      {headers: defaultHeaders, tags: { rawlsApi: 'batchUpsert', feature: 'insert', size: largeUpsertSize }});
    check(res4, { "status is 204": (res) => res.status === 204 });
    sleep(.1);

    // re-generate the small batchUpsert payload to get some changes, and update it
    const largeUpdate = generateBatchUpsert(testType, largeUpsertSize);
    const res5 = http.post(
      `${workspaceRoot(__ENV.TEST_GROUP)}/entities/batchUpsert`,
      JSON.stringify(largeUpdate),
      {headers: defaultHeaders, tags: { rawlsApi: 'batchUpsert', feature: 'update', size: largeUpsertSize }});
    check(res5, { "status is 204": (res) => res.status === 204 });
    sleep(.1);

    // delete by type
    const res6 = http.del(
      `${workspaceRoot(__ENV.TEST_GROUP)}/entityTypes/${testType}`, null,
      {headers: defaultHeaders, tags: { rawlsApi: 'deleteEntitiesOfType' }});
    check(res6, { "status is 204": (res) => res.status === 204 });
    sleep(.1);
  });
}


/**********************************************************************
 * entity type metadata
 **********************************************************************/

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

/**********************************************************************
 * before-all / after-all callbacks
 **********************************************************************/
// setup code, such as inserting test data before the test scenarios run
export function setup() {}

// teardown code, deletes test data after the test scenarios run
export function teardown(data) {

  // batchUpsertTest should clean up after itself; it's handled here too just in case
  const testGroups = ['baseline', 'test'];
  const tablesToDelete = ['loadTestPuts', 'batchUpsertTest'];

  for (const testGroup of testGroups) {
    for (const table of tablesToDelete) {
      console.log(`deleting table '${table}' for test group '${testGroup}' ...`)
      const statusCode = deleteTable(testGroup, table);
      console.log(`... deleteTable ${testGroup}/${table} result is ${statusCode}`)
    }
  }
}

// delete all entities of a given type
export function deleteTable(testGroup, tableName) {
  let res = http.del(
      `${workspaceRoot(testGroup)}/entityTypes/${tableName}`, null, defaultParams);
  return res.status;
}

/**********************************************************************
 * helper functions
 **********************************************************************/

// auth using a bearer token and send/receive JSON
const defaultHeaders = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${__ENV.QUICKSILVER_USER_TOKEN}`
  };

// default request parameters: include the default headers
const defaultParams = {headers: defaultHeaders}

// determine the workspace for the current request based on the testGroup
const workspaceRoot = (testGroup) => {
  // take everything up to the first comma, allowing for "annotations" to the testGroup
  const groupBase = testGroup.split(",")[0];
  return `${__ENV.QUICKSILVER_TERRA_INSTANCE}/api/workspaces/${__ENV.QUICKSILVER_WS_PREFIX}${groupBase}`;
}

