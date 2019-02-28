'use strict';

/**
 * This file contains a class which can be used to help manage Postgres
 * databases for the purpose of writing unit tests.  Using a few Postgres
 * utilities (postmaster, initdb), it will create and initialise a database for
 * unit tests.
 *
 * Each unit test based on Postgres should have an empty database to start
 * from, and the developer should not need to manually manage the database used
 * for unit tests.  Furthermore, a system managed database should not be
 * affected by running unit tests.
 *
 * None of this code is designed to be a part of a production deployment,
 * however is exported publicly so that people using this library can have test
 * database management.
 *
 * Example usage (in a Mocha test):
 *
 *   let datadir = 'my-database';
 *   let helper = new PGTestDatabase({datadir});
 *
 *   before(async () => {
 *     await helper.configurePostgresDatadir();
 *     await helper.startPostgres();
 *   });
 *
 *   beforeEach(async() => {
 *     await helper.initialiseDatabase();
 *   });
 *
 *   after(async () => {
 *     await helper.stopPostgres();
 *     // delete datadir with something like rimraf
 *   });
 *
 *   it('should connect to database', async () => {
 *     let client = new pg.Client(helper.clientConnectionString());
 *     await client.connect();
 *     console.dir(await client.query('select version()'));
 *     await client.end();
 *   });
 *
 */

const log = require('./log')('db-helper');

const util = require('util');
const _fs = require('fs');

const fs = {
  mkdir: util.promisify(_fs.mkdir),
  appendFile: util.promisify(_fs.appendFile),
  createWriteStream: _fs.createWriteStream,
};
const path = require('path');
const {execFile, spawn} = require('child_process');

const pg = require('pg');

const escapeIdentifier = pg.Client.prototype.escapeIdentifier;

// Wrap running a process and dump debugging information on failure
async function run({args, returnOutput=false, quiet=false}) {
  let options = {};

  return new Promise((resolve, reject) => {
    execFile(args[0], args.slice(1), options, (err, stdout, stderr) => {
      if (err) {
        // Consider adding information about cwd, env
        log.info([
          `ERROR RUNNING COMMAND: ${JSON.stringify(args)}`,
          `STDOUT: ${stdout}`,
          `STDERR: ${stderr}`,
        ].join('\n'));
        return reject(err);
      }
      if (returnOutput) {
        return resolve({stdout, stderr});
      } else {
        return resolve();
      }
    });
  });
}

// Parse a postgres version into a string.  Supports format 90601 and '9.6.1'
// with equivalent returns of 90601
function parsePGVersion(input) {
  if (typeof input === 'number') {
    return input;
  }
  let [breaking, feature, fix] = input.split('.');
  if (feature > 99 || fix > 99) {
    throw new Error(`Provided version ${input} is not valid`);
  }
  let ver = Number.parseInt(breaking, 10) * 10000;
  ver += Number.parseInt(feature, 10) * 100;
  ver += Number.parseInt(fix);

  log.trace(`Parsed ${input} to ${ver}`);
  return ver;
}

/**
 * PGTestDatabase is used to help manage development postgres environments
 * for unit testing lib-psql derived projects.  This class understands
 * how to initialize a testing database, start an instance and shut it down.
 *
 * It also understands how to run queries against the database using:
 *
 *  1. node-pg string queries
 *  2. psql cli string queries
 *  3. psql file input (e.g. a .sql file)
 *
 * This class does the most simple SQL querying as possible to ensure that it
 * does not become overly complex.  As a result, no connection pooling is
 * performed and all parameterization is done manually
 *
 * Roughly, this class understands how to run `initdb`, `createuser`,
 * `createdb` and the postgres daemon process (`postmaster`).
 *
 * Because the postgres project does not make official non-packaged binary
 * packages available, this class does not make an attempt to manage the actual
 * binaries.  This means that the system must provide binaries.  This does not
 * preclude the use of alternate binaries outside the path, rather binaries can
 * be set to specific paths as constructor arguments
 *
 * Specifically it requires the following binaries:
 *   1. `postmaster` to be able to start a Postgres DB
 *   2. `initdb` to be able to build the data directory (PGDATA)
 *   3. `psql` (optional) to be able to run PSQL queries or .sql files
 *
 * The constructor provides facilities to limit the versions of Postgres which
 * are supported for this instance.  They will be specified in the format X.Y.Z
 * (e.g. 9.6.3) and will be converted by this class into the internal
 * comparison format (e.g. 90603).  This error is only thrown when attempting
 * to start the database server.
 *
 * Each time the database is started, its state is erased.  The state is
 * maintained when the database is stopped.
 *
 */
class PGTestDatabase {

  constructor({
    // Directory where the postgres data directory (PGDATA) will be
    // located
    datadir,
    // Port which Postgres will listen on
    port,
    // If passed, prefix is added to the name of program being used.  For
    // example, if the prefix is passed as '/usr/bin/' the postmaster binary
    // would be used as '/usr/bin/postmaster'.  A specific postmaster binary
    // path will override this value and an undefined value will result in
    // using the bare name of the program (e.g. 'postmaster')
    prefix=process.env.POSTGRES_PREFIX || undefined,
    // Full path to postmaster, initdb and psql binaries (optional);
    postmaster=process.env.POSTGRES_POSTMASTER,
    initdb=process.env.POSTGRES_INITDB,
    psql=process.env.POSTGRES_PSQL,
    // Name of the database which will be created for testing
    dbname = 'testing',
    // Name of the unprivileged user which will be created for testing
    dbuser = 'testing',
    // Minimum and Maximum acceptable server versions.  These can be specified
    // as a number (e.g. 90500) or as a string (e.g. '9.5.0') and are inclusive
    // bounds on the acceptable versions for the database.  The default value
    // is no olde than 9.5 and no restriction on maximum version
    maxver,
    minver = '9.5.0',
  }) {
    if (typeof datadir !== 'string') {
      throw new Error('Data Dir must be string');
    }
    if (path.isAbsolute(datadir)) {
      this.datadir = datadir;
    } else {
      this.datadir = path.join(process.cwd(), datadir);
    }
    this.sockdir = path.join(this.datadir, 'run');

    if (port) {
      this.port = port;
    } else if (process.env.PGPORT) {
      this.port = Number.parseInt(process.env.PGPORT);
    } else {
      this.port = Math.floor(Math.random() * 10000) + 50000;
    }
    this.dbname = dbname;
    this.dbuser = dbuser;

    if (maxver) {
      this.maxver = parsePGVersion(maxver);
    }

    if (minver) {
      this.minver = parsePGVersion(minver);
    }

    // Default to using the bare command name
    this.postmaster = 'postmaster';
    this.initdb = 'initdb';
    this.psql = 'psql';

    // But if a prefix is specified, overwrite the bare command name with the
    // prefixed command
    if (prefix) {
      this.postmaster = path.join(prefix, 'postmaster');
      this.initdb = path.join(prefix, 'initdb');
      this.psql = path.join(prefix, 'psql');
    }

    // Finally, if there's a full path to any command, overwrite the prefixed
    // or bareword version
    if (postmaster) {
      this.postmaster = postmaster;
    }
    if (initdb) {
      this.initdb = initdb;
    }
    if (psql) {
      this.psql = psql;
    }

    this.dbproc = undefined;
  }

  /**
   * Start the Postgres server running.  This method does not do any
   * initialisation of the database.  To create the Postgres database files,
   * you can run PGTestDatabase.prototype.configurePostgresDatadir().  To create the
   * SQL databse, you can run PGTestDatabase.prototype.initialiseDatabase().
   *
   * There are two special environment variables:
   *   1. POSTGRES_LOG_STDIO -- if set to any value, mix Postgres logs into
   *      standard i/o
   *   2. POSTGRES_LOG_FILE -- specify a custom file to write Postgres logs
   *      into
   * In the absence of either environment variable, the logs will be written to
   * a file 'postgres.log' in the root of the Postgres data directory.  For
   * example, `$PGDATA/postgres.log`
   */
  async startPostgres() {
    if (this.dbproc) {
      throw new Error('Cannot start a postgres server which is already running');
    }
    log.debug('Starting Postgres');

    let procLog;

    if (process.env.POSTGRES_LOG_STDIO) {
      procLog = 'inherit';
    } else {
      let logfile = process.env.POSTGRES_LOG_FILE || path.join(this.datadir, 'postgres.log');
      log.info(`Writing Postgres logs to ${logfile}`);
      procLog = fs.createWriteStream(logfile);

      // We want to wait until the log file is ready for writing before
      // continuing
      await new Promise(resolve => {
        procLog.once('ready', () => {
          log.debug(`Postgres log file ${logfile} is ready for writing`);
          resolve();
        });
      });
    }

    this.dbproc = spawn(this.postmaster, ['-D', this.datadir], {
      stdio: ['ignore', procLog, procLog],
    });

    await new Promise(done => setTimeout(done, 100));

    let client = await this.getSuperuserClient();

    // Ensure that the database which is connected to is a minimum version of
    // Postgres.  Because actually managing the installation of Postgres is out
    // of scope for this class, the system Postgres must be used.  Ensuring
    // that a correct version of Postgres is available is critical
    try {
      log.debug('Determining version of Postgres');
      let result = await client.query('SHOW server_version_num');
      let serverVersion = Number.parseInt(result.rows[0].server_version_num);
      log.info(`Connected to Postgres ${serverVersion}`);

      if (this.maxver && serverVersion > this.maxver) {
        throw new Error(`Server version ${serverVersion} exceeds maximum allowed ${this.maxver}`);
      }

      if (this.minver && serverVersion < this.minver) {
        throw new Error(`Server version ${serverVersion} is below minimum allowed ${this.minver}`);
      }
    } finally {
      await client.end();
    }

    await this.initialiseDatabase();

    log.info('Postgres server launched successfully');
  }

  /**
   * Terminate the Postgres server.  Initially try to do a clean shutdown by
   * sending the SIGTERM signal to the Postgres server process.  If, after
   * three seconds, the server has not reported itself as ended the server will
   * be terminated with the SIGKILL signal.  Because the server is never moved
   * into its own process group, exiting this node process should end the
   * database
   */
  async stopPostgres() {
    if (!this.dbproc) {
      log.info('Postgres already stopped');
      return;
    }
    log.debug('Stopping Postgres');

    await new Promise((resolve, reject) => {

      // Send SIGKILL if the database takes more than 3s to end
      let forceKill = setTimeout(() => {
        log.warn('Force killing Postgres');
        log.debug('Sending SIGKILL to Postgres');
        this.dbproc.kill('SIGKILL');
        return resolve();
      }, 3000);

      this.dbproc.once('exit', async (code, signal) => {
        if (code && code !== 0) {
          return reject(new Error(`Postgres server process ended with ${code}`));
        }

        log.info('Stopped Postgres');
        this.dbproc = undefined;

        clearTimeout(forceKill);

        return resolve();
      });

      log.debug('Sending SIGTERM to Postgres');
      this.dbproc.kill();
    });

    log.info([
      'You can restart this database manager for debugging by running:',
      `   "${this.postmaster} -D ${this.datadir}"`,
      'And connect to it with',
      `   User: "${this.psql} -h 127.0.0.1 -p ${this.port} -d ${this.dbname} -U ${this.dbuser}"`,
      `   Admin: "${this.psql} -h 127.0.0.1 -p ${this.port} -d ${this.dbname} -U postgres"`,
    ].join('\n'));
  }

  /**
   * Create a Postgres data directory.  This is commonly refered to by the
   * PGDATA environment variable and is the on-disk storage for a Postgres
   * installation.
   *
   * The configuration of this database is not recommended for any production
   * deployment, but rather for doing testing.  Configuring a production
   * postgres instance is outside the scope of this library
   */
  async configurePostgresDatadir() {
    log.debug(`Configuring Postgres data directory ${this.datadir}`);

    await fs.mkdir(this.datadir);

    await run({
      args: [
        this.initdb,
        '-D', this.datadir,
        '--auth=trust',
        '--encoding=UTF8',
        '--locale=en_US.UTF8',
        '--username=postgres',
      ],
    });

    log.debug('Created Postgres data directory');

    await fs.mkdir(this.sockdir);

    await fs.appendFile(path.join(this.datadir, 'postgresql.conf'), [
      "log_statement = 'all'",
      "log_connections = on",
      "log_disconnections = on",
      "log_duration = on",
      "log_filename = 'postgres.log'",
      "logging_collector = off",
      "log_destination = stderr",
      "log_lock_waits = on",
      "max_connections = 100",
      `unix_socket_directories = '${this.sockdir}'`,
      `port = ${this.port}`,
    ].join('\n'));
    log.info(`Successfully configured Postgres server`);
  }

  /**
   * Create an empty SQL database(`CREATE DATABASE` not a `PGDATA` directory),
   * deleting an existing database with the same name if present
   */
  async initialiseDatabase() {
    let client;
    try {
      client = await this.getSuperuserClient();
      log.debug(`Initialising ${this.dbname}`);

      // Since the drop database command in Postgres does not support the
      // cascade option, instead find all schemas and drop all objects in those
      // schemas before deleting the database
      let result = await client.query([
        'SELECT schema_name FROM information_schema.schemata',
        'WHERE schema_name <> \'information_schema\'',
        'AND schema_name NOT LIKE \'pg_%\'',
      ].join(' '));
      for (let schema of result.rows.map(x => x.schema_name)) {
        await client.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
      }

      await client.query(`DROP DATABASE IF EXISTS ${escapeIdentifier(this.dbname)}`);
      await client.query(`DROP ROLE IF EXISTS ${escapeIdentifier(this.dbuser)}`);
      await client.query(`CREATE ROLE ${escapeIdentifier(this.dbuser)} WITH LOGIN`);
      await client.query(`CREATE DATABASE ${escapeIdentifier(this.dbname)} WITH OWNER ${escapeIdentifier(this.dbuser)}`);
      log.info(`Finished initialising ${this.dbname}`);
    } finally {
      await client.end();
    }
  }

  /**
   * Return a client connection to the database which has already connected.
   * This client must have the `.end()` method called on it when no longer
   * needed
   */
  async getClient() {
    let client = new pg.Client(this.clientConnection());
    await client.connect();
    return client;
  }

  /**
   * Return a superuser connection to the database which has already connected.
   * This client must have the `.end()` method called on it when no longer
   * needed
   */
  async getSuperuserClient(testDB) {
    let client = new pg.Client(this.superuserConnection(testDB));
    await client.connect();
    return client;
  }
  /**
   * Run a query using a Javascript client to the database
   */
  async runQuery(query) {
    return this._runQuery(await this.getClient(), query);
  }

  /**
   * Run a query using a Javascript superuser client to the database
   */
  async runSuperuserQuery(query, testDB) {
    return this._runQuery(await this.getSuperuserClient(testDB), query);
  }

  /**
   * Underlying implementation for running js client queries
   */
  async _runQuery(client, query) {
    if (!this.dbproc) {
      throw new Error(`Queries can only run on started dbs ${query}`);
    }
    log.debug(`Running query ${JSON.stringify(query)}`);

    try {
      return await client.query(query);
    } finally {
      log.debug(`Finished query ${JSON.stringify(query)}`);
      await client.end();
    }
  }

  // Run a command using the psql command line tool
  async runPSQLCommand(command) {
    if (!this.dbproc) {
      throw new Error(`PSQL Commands can only run on started dbs ${query}`);
    }

    log.debug(`Running PSQL command ${command}`);
    return await run({
      args: [
        this.psql,
        '-h', '127.0.0.1',
        '-p', this.port,
        '-U', 'postgres',
        '-c', command,
        '-d', this.dbname,
        '-b',
      ],
      returnOutput: true,
    });
  }

  // Run a .sql file using the psql command line tool
  async runPSQLFile(file) {
    if (!this.dbproc) {
      throw new Error(`PSQL Files can only run on started dbs ${query}`);
    }
    log.debug(`Running PSQL file ${file}`);
    return await run({
      args: [
        this.psql,
        '-h', '127.0.0.1',
        '-p', this.port,
        '-U', 'postgres',
        '-f', file,
        '-d', this.dbname,
        '-b',
      ],
      returnOutput: true,
    });
  }

  // Note that for use with PG-Pool you'll probably wish to use idleTimeoutMillis
  clientConnection() {
    return {
      host: '127.0.0.1',
      user: this.dbuser,
      port: this.port,
      database: this.dbname,
    };
  }

  clientConnectionString() {
    return `postgres://${this.dbuser}@127.0.0.1:${this.port}/${this.dbname}`;
  }

  superuserConnection(testDB) {
    return {
      host: '127.0.0.1',
      user: 'postgres',
      port: this.port,
      database: testDB ? this.dbname : 'postgres',
    };
  }

  superuserConnectionString(testDB) {
    return `postgres://postgres@127.0.0.1:${this.port}/${testDB ? this.dbname : 'postgres'}`;
  }
}

module.exports = {PGTestDatabase, run};
