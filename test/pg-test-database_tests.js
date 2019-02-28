'use strict';

const assume = require('assume');
const tmp = require('tmp');
const fs = require('fs');
const path = require('path');
const rimraf = require('rimraf').sync;
const pg = require('pg');

const {PGTestDatabase, run} = require('../lib/pg-test-database');

describe('Postgres Test Database', () => {
  describe('commands', () => {
    it('should run a successful command', async () => {
      await run({args: ['uname', '-a']});
    });

    it('should run a failing command', async () => {
      await run({args: ['bash', '-c', 'exit 0']});
      await assume(run({
        args: ['bash', '-c', 'exit 1'],
      })).rejects();
    });
  });

  describe('database management', function() {
    this.timeout(10 * 1000);

    let subject;
    let datadir = process.env.PGDATA || tmp.tmpNameSync();

    beforeEach(async () => {
      rimraf(datadir);
      subject = new PGTestDatabase({datadir});
      await subject.configurePostgresDatadir();
    });

    afterEach(async () => {
      await subject.stopPostgres();
    });

    it('should be able to communicate with the postgres server', async () => {
      fs.statSync(path.join(datadir, 'postgresql.conf'));
      await subject.startPostgres();
      // A simple query, but shows communication with the postgres server
      // nonetheless
      let result = await subject.runQuery("SELECT 'john' AS name");
      assume(result.rows).lengthOf(1);
      assume(result.rows[0]).has.property('name', 'john');
    });

    it('should not start with too low version', async () => {
      subject.minver = Number.MAX_SAFE_INTEGER;
      await assume(subject.startPostgres()).rejects(/below minimum/);
    });

    it('should not start with too high version', async () => {
      subject.maxver = 1;
      await assume(subject.startPostgres()).rejects(/exceeds maximum/);
    });

    it('should generate valid user connection string', async () => {
      await subject.startPostgres();

      let client = new pg.Client(subject.clientConnectionString());
      try {
        await client.connect();
        await client.query('select now()');
      } finally {
        await client.end();
      }
    });

    it('should generate valid superuser connection string', async () => {
      await subject.startPostgres();

      let client = new pg.Client(subject.superuserConnectionString());
      try {
        await client.connect();
        await client.query('select now()');
      } finally {
        await client.end();
      }
    });

    it('should generate valid user connection object', async () => {
      await subject.startPostgres();

      let client = new pg.Client(subject.clientConnection());
      try {
        await client.connect();
        await client.query('select now()');
      } finally {
        await client.end();
      }
    });

    it('should generate valid superuser connection object', async () => {
      await subject.startPostgres();
      let client = new pg.Client(subject.superuserConnection());
      try {
        await client.connect();
        await client.query('select now()');
      } finally {
        await client.end();
      }
    });

    it('can run a sql command with psql', async () => {
      await subject.startPostgres();
      await subject.runPSQLCommand('SELECT now() as now');
    });

    it('can run a sql file with psql', async () => {
      await subject.startPostgres();
      await subject.runPSQLFile(__dirname + '/test.sql');
    });
  });
});
