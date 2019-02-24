'use strict';

const assume = require('assume');
const tmp = require('tmp');
const rimraf = require('rimraf').sync;
const fs = require('fs');
const path = require('path');
const pg = require('pg');

const {PGTestDatabase} = require('../lib/pg-test-database');
const {PG, SQL} = require('../lib/pg');

describe('PG', () => {
  let datadir;
  let db; 
  let subject;

  before(async function() {
    this.timeout(10*1000);

    let datadir = process.env.PGDATA || tmp.tmpNameSync();

    rimraf(datadir);

    db = new PGTestDatabase({datadir});

    await db.configurePostgresDatadir();
    await db.startPostgres();
  });

  beforeEach(async function() {
    await db.initialiseDatabase();
    let connopts = db.clientConnection();
    // This setting is important because without it the idle timeout would hold
    // the mocha tests open for the default of 10s
    connopts.idleTimeoutMillis = 1000;
    subject = new PG(connopts);
  });

  after(async function() {
    this.timeout(10*1000);
    await db.stopPostgres();
    await subject.close();
  });

  it('should be able to open and close the pool', async () => {
    await subject.query('select now()');
  });

  it('should be able to checkout a client and run a query', async () => {
    let client = await subject._checkoutClient();
    await client.query('select now()');
    client.release();
  });

  it('should be able to run a query', async () => {
    await subject.query('select now()');
  });

  it('should reject when a query fails', async () => {
    try {
      await subject.query('SELECT <> 123');
      return Promise.reject(new Error('should fail'));
    } catch (err) {
      if (!/syntax error at or near "<>"/.test(err.message || err)) {
        throw err;
      }
    }
  });

  it('should persist values set in committed transaction', async () => {
    await subject.query('CREATE TABLE test1 (col1 INTEGER PRIMARY KEY);');

    let tx = await subject.tx();
    await tx.query('INSERT INTO test1 (col1) VALUES (1)');
    await tx.success();

    let result = await subject.query('SELECT * FROM test1');

    assume(result.rows).deeply.equals([{col1: 1}]);
  });

  it('should not persist values set in a rolled back transaction', async () => {
    await subject.query('CREATE TABLE test1 (col1 INTEGER PRIMARY KEY);');
    await subject.query('INSERT INTO test1 (col1) VALUES (1)');

    let tx = await subject.tx();
    await tx.query('UPDATE test1 SET col1 = 2');
    try {
      await tx.failure(new Error('lala'));
      return Promise.reject(new Error('should fail'));
    } catch (err) {
      if (!/^lala$/.test(err.message || err)) {
        throw err;
      }
    }

    let result = await subject.query('SELECT * FROM test1');

    assume(result.rows).deeply.equals([{col1: 1}]);
  });

  describe('running functions in a transaction', () => {
    it('should be able to persist values', async () => {
      await subject.query('CREATE TABLE test1 (col1 INTEGER PRIMARY KEY)');

      await subject.runInTx(async tx => {
        await tx.query('INSERT INTO test1 (col1) VALUES (1);');
      });

      let result = await subject.query('SELECT * FROM test1');

      assume(result.rows).deeply.equals([{col1: 1}]);
    });

    it('should rollback a transaction when a non-sql error is thrown', async () => {
      await subject.query('CREATE TABLE test1 (col1 INTEGER PRIMARY KEY)');
      await subject.query('INSERT INTO test1 (col1) VALUES (1);');

      try {
        await subject.runInTx(async tx => {
          await tx.query('UPDATE test1 SET col1 = 2');
          throw new Error('lala');
        });
      } catch (err) {
        if (!/^lala$/.test(err.message || err)) {
          throw err;
        }
      }

      let result = await subject.query('SELECT * FROM test1');

      assume(result.rows).deeply.equals([{col1: 1}]);
    });
  });

  it('should work with parameterization', async () => {
    await subject.query('CREATE TABLE test (col1 INTEGER PRIMARY KEY);');
    await subject.query(SQL`INSERT INTO test`.append(SQL`(col1) VALUES (${1});`));

    let result = await subject.query('SELECT * FROM test');

    assume(result.rows).deeply.equals([{col1: 1}]);
  });

  it('should be able to escape identifiers', async () => {
    await subject.query('CREATE TABLE test (col1 INTEGER PRIMARY KEY);');
    await subject.query(SQL`INSERT INTO`
      .append(subject.escapeIdentifier('test'))
      .append(SQL`(col1) VALUES (${1});`));

    let result = await subject.query('SELECT * FROM test');

    assume(result.rows).deeply.equals([{col1: 1}]);
  });

  it('should be able to escape literals', async () => {
    await subject.query('CREATE TABLE test (col1 TEXT PRIMARY KEY);');
    let x = "'john'); DROP TABLE test;"
    await subject.query(`INSERT INTO test (col1) VALUES (${subject.escapeLiteral(x)});`);

    let result = await subject.query('SELECT * FROM test');

    assume(result.rows).deeply.equals([{col1: x}]);
  });

  it('should be able to curse over a table', async () => {
    await subject.query('CREATE TABLE test (col1 INTEGER PRIMARY KEY);');
    for (let i = 0; i < 10; i++) {
      await subject.query(SQL`INSERT INTO test`.append(SQL`(col1) VALUES (${i});`));
    }

    let rows = [];

    await subject.curse(SQL`SELECT * FROM test`, row => {
      rows.push(row.col1); 
    });

    assume(rows).deeply.equals([0,1,2,3,4,5,6,7,8,9]);
  });

  it.only('should be able to handle a rowFunc which throws', async () => {
    await subject.query('CREATE TABLE test (col1 INTEGER PRIMARY KEY);');
    for (let i = 0; i < 10; i++) {
      await subject.query(SQL`INSERT INTO test`.append(SQL`(col1) VALUES (${i});`));
    }

    let rows = [];

    await assume(async () => {
      await subject.curse(SQL`SELECT * FROM test`, row => {
        throw new Error('hi');
      });
    }).rejects();

  });

  it.only('should be able to handle a rowFunc which rejects', async () => {
    await subject.query('CREATE TABLE test (col1 INTEGER PRIMARY KEY);');
    for (let i = 0; i < 10; i++) {
      await subject.query(SQL`INSERT INTO test`.append(SQL`(col1) VALUES (${i});`));
    }

    let rows = [];

    await assume(async () => {
      await subject.curse(SQL`SELECT * FROM test`, async row => {
        return Promise.reject(new Error('hi'));
      });
    }).rejects();

  });

  it('should be able to curse over a table with a batch size', async () => {
    await subject.query('CREATE TABLE test (col1 INTEGER PRIMARY KEY);');
    for (let i = 0; i < 10; i++) {
      await subject.query(SQL`INSERT INTO test`.append(SQL`(col1) VALUES (${i});`));
    }

    let rows = [];

    await subject.curse(SQL`SELECT * FROM test`, row => {
      rows.push(row.col1); 
    }, {batchSize:1});

    assume(rows).deeply.equals([0,1,2,3,4,5,6,7,8,9]);
  });

  it('should be able to curse over a table with an async function', async () => {
    await subject.query('CREATE TABLE test (col1 INTEGER PRIMARY KEY);');
    for (let i = 0; i < 10; i++) {
      await subject.query(SQL`INSERT INTO test`.append(SQL`(col1) VALUES (${i});`));
    }

    let rows = [];

    await subject.curse(SQL`SELECT * FROM test`, async row => {
      return new Promise(resolve => {
        process.nextTick(() => {
          rows.push(row.col1);
          resolve();
        })
      });
    });

    assume(rows).deeply.equals([0,1,2,3,4,5,6,7,8,9]);
  });
  
  it('should be able to curse over a table with a sequential async function', async () => {
    await subject.query('CREATE TABLE test (col1 INTEGER PRIMARY KEY);');
    for (let i = 0; i < 10; i++) {
      await subject.query(SQL`INSERT INTO test`.append(SQL`(col1) VALUES (${i});`));
    }

    let rows = [];

    await subject.curse(SQL`SELECT * FROM test`, async row => {
      return new Promise(resolve => {
        process.nextTick(() => {
          rows.push(row.col1);
          resolve();
        })
      });
    }, {sequential: true});

    assume(rows).deeply.equals([0,1,2,3,4,5,6,7,8,9]);
  });
  
  it('should be able to curse over a table with a limit', async () => {
    await subject.query('CREATE TABLE test (col1 INTEGER PRIMARY KEY);');
    for (let i = 0; i < 10; i++) {
      await subject.query(SQL`INSERT INTO test`.append(SQL`(col1) VALUES (${i});`));
    }

    let rows = [];

    await subject.curse(SQL`SELECT * FROM test`, row => {
      rows.push(row.col1)
    }, {limit: 4});

    assume(rows).deeply.equals([0,1,2,3]);
  });
});
