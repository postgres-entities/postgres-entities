'use strict';

const {PG, PGTestDatabase} = require('../');
const {parse: parseConnstr} = require('pg-connection-string');
const fs = require('fs');
const url = require('url');

/**
 * This script initialises a database.  When run on a service which provides a
 * postgres connection string in the DATABASE_URL environment variable, only
 * schema creation occurs.  For other environments, a new database will be
 * initialised using PGTestDatabase.
 */
async function setup() {
  if (process.env.DATABASE_URL) {
    // Assumption here is that any database url which is not local is SSL
    let urlparts = new URL(process.env.DATABASE_URL);
    if (urlparts.hostname !== '127.0.0.1' && urlparts.hostname !== 'localhost') {
      urlparts.searchParams.set('ssl', true);
      urlparts.searchParams.set('min', 40);
      urlparts.searchParams.set('max', 100);
    }
    return urlparts.toString();
  } else {
    const PGDATA = process.env.PGDATA || 'example-service-pgdir';
    const PGPORT = '5454';
 
    // In order to avoid needing to set up a database locally, we'll use the
    // DBHelper for this test database.  This is included so that the example can
    // be run without having to do any local database setup
    const testDB = new PGTestDatabase({
      datadir: PGDATA,
      dbname: 'todo-list',
      port: PGPORT,
    });

    // Only create the database if it doesn't already exist
    if (!fs.existsSync(testDB.datadir)) {
      // Initialise and start the Postgres databsae
      await testDB.configurePostgresDatadir();
    } 

    // Start and initialise the database.  For this simple example
    // service, we start with a fresh state each execution
    await testDB.startPostgres();
    await testDB.initialiseDatabase();

    return testDB.superuserConnectionString(true);
  }
}

module.exports = {setup};

