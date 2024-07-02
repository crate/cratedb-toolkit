/*
Example program using the official Rockset Javascript/Typescript Client with a CrateDB backend.
Here: "Documents" and "Queries" APIs, basic usage.

Usage
=====
- Install dependencies: Run `npm install`.
- Adjust configuration: Define `ROCKSET_APISERVER` environment variable.
- Run program: Run `npm run basic`.

Documentation
=============
- https://github.com/rockset/rockset-js
- https://github.com/rockset/rockset-js/tree/master/packages/client
- https://docs.rockset.com/documentation/reference/adddocuments
- https://docs.rockset.com/documentation/reference/query
*/
import rockset from '@rockset/client';

// Connect to API endpoint.
const rockset_apiserver = process.env.ROCKSET_APISERVER || "http://127.0.0.1:4243";
const rockset_apikey = process.env.ROCKSET_APIKEY || "abc123";
const rocksetClient = rockset.default(rockset_apikey, rockset_apiserver);

// Add documents into your collection, using the Write API.
// https://github.com/rockset/rockset-js/tree/master/packages/client#add-documents-into-your-collection-with-the-write-api
rocksetClient.documents
  .addDocuments(
    'commons' /* name of workspace */,
    'users' /* name of collection */,
    {
      /* array of JSON objects, each representing a new document */
      data: [
        { first_name: 'Brian', last_name: 'Morris', age: '14' },
        { first_name: 'Justin', last_name: 'Smith', age: '78' },
        { first_name: 'Scott', last_name: 'Wong', age: '42' },
      ],
    }
  )
  .then(console.log)
  .catch(console.error);

// Submit SQL query.
// https://github.com/rockset/rockset-js/tree/master/packages/client#run-a-sql-query
rocksetClient.queries
  .query({
    sql: {
      query: 'SELECT * FROM commons.users u WHERE u.age > :minimum_age',
      /* (optional) list of parameters that may be used in the query */
      parameters: [
        {
          name: 'minimum_age' /* name of parameter */,
          type: 'int' /* one of: int, float, bool, string date, datetime, time, timestamp */,
          value: '20' /* value of parameter */,
        },
      ],
      default_row_limit: 150 /* (optional) row limit to be used if no limit is specified in the query */,
    },
  })
  .then(console.log)
  .catch(console.error);
