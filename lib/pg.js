'use strict';

const log = require('./log')('pg');

const {EventEmitter} = require('events');
const {Pool, Client, types} = require('pg');
const Cursor = require('pg-cursor');
const {SQL} = require('sql-template-strings');

/**
 * Since Javascript now supports arbitrarily large integers,
 * we'll parse the int8 datatype into a BigInt
 */
types.setTypeParser(20, function(val) {
  return BigInt(val)
})

class ErrRollback extends Error {};

class PG extends EventEmitter {
  constructor(...args) {
    super()
    this._pool = new Pool(...args);
    this._pool.on('error', (...x) => this.emit('error', ...x));
    
    // I'd like to have something which periodically calls the
    // Pool.prototype._isFull() method to log when the pool is exhausted along
    // with a message like "The postgres client pool is currently exhausted.
    // This could signify a need for more connections to the database or that a
    // client has been checked out but not released".
  }

  /**
   * Check out a raw PG client from the pool.  This is not for use outside of
   * this module
   */
  async _checkoutClient(...args) {
    // I would like to have something which adds all clients to a list of
    // clients which are checked out as well as the time of checkout so that we
    // can easily log when a client lasts for a very long amount of time.
    log.trace('checked out client');
    return this._pool.connect(...args);
  }

  /**
   * Close the pool
   */
  async close(...args) {
    log.trace('closing pool');
    return this._pool.end(...args);
  }

  /**
   * Run a query without a transaction
   */
  async query(...args) {
    log.debug('running query %j', args);
    return this._pool.query(...args);
  }

  /**
   * Initiate a transaction
   */
  async tx() {
    log.trace('opening transaction');
    let tx = new Tx(await this._checkoutClient());
    await tx.open();
    return tx;
  }

  /**
   * Curse over any entire table and run rowFunc on the row's values.
   *
   * The options are:
   *   - limit: maximum invocations of rowFunc
   *   - batchSize (default 100): how many rows to fetch from the default at a
   *   time
   *   - sequential (default false): For promise returning functions, a true
   *   value for this option means that each rowFunc invocation will wait for
   *   the previous invocation to resolve/reject before starting.  If this
   *   value is false, all promises will be awaited once per batch of rows
   *
   * TODO Write tests for this
   * TODO consider writing an option for rowFuncs which return a promise so
   * that the can sequentially or concurrently awaited.
   * TODO Consider writing an option which sets the batch size of each .read()
   * call
   */
  async curse(query, rowFunc, {limit, batchSize=100, sequential=false}={}) {
    let client = await this._checkoutClient();

    let _cursor = new Cursor(query.text, query.values);
    let cursor = await client.query(_cursor);

    // Let's support > 2^53+1 rows not because we have to, but because we can
    if (typeof limit === 'number') {
      limit = BigInt(limit);
    }

    let nRows = 0n;

    try {
      do {
        let rows = await new Promise((resolve, reject) => {
          cursor.read(batchSize, (err, rows) => {
            if (err) {
              return reject(err);
            }
            return resolve(rows);
          });
        });

        // Having zero rows in the response is how the cursor signals completion
        if (rows.length === 0) {
          break;
        }

        let promises = [];

        // The rowFunc will be called on each individual row up to the limit
        // specified.  This might mean that more than limit rows are fetched, but
        // never more than limit rows are passed to the rowFunc
        for (let row of rows) {
          // Async functions should be handled correctly.
          let x;
          x = rowFunc(row);

          if (x && typeof x.then === 'function') {
            if (sequential) {
              await x;
            } else {
              promises.push(x);
            }
          }

          nRows++;

          if (nRows >= limit) {
            await new Promise((resolve, reject) => {
              cursor.close(err => {
                if (err) {
                  return reject(err);
                }
                return resolve();
              });
            });

            break;
          }
        }

        if (promises.length > 0) {
          await Promise.all(promises);
        }

      } while (true);

    } finally {
      client.release();
    }
  }  
  /**
   * Run a promise returning function
   */
  async runInTx(func) {
    log.trace('running function in transaction');
    let tx = await this.tx();
    try {
      let result = await func(tx);
      await tx.success();
      return result;
    } catch (err) {
      await tx.failure(err);
    }
  }

  escapeIdentifier (...x) {
    return Client.prototype.escapeIdentifier(...x);
  }

  escapeLiteral (...x) {
    return Client.prototype.escapeLiteral(...x);
  }
}
PG.escapeIdentifier = function (...x) {
  return Client.prototype.escapeIdentifier(...x);
}

PG.escapeLiteral = function(...x) {
  return Client.prototype.escapeLiteral(...x);
}


class Tx {
  constructor(client){
    this._client = client;
  }

  // Open a transaction by checking a client out of the pool.  Also set up a
  // timeout to notify users of long living transactions
  async open() {
    this._lastQuery = [];
    this._nag = setTimeout(() => {
      log.warn(`A transaction has been alive for 5s, last ran ${JSON.stringify(this.lastQuery)}`);
    }, 5000);

    await this._client.query('BEGIN');
  }

  // Run a query in the transaction.  If running any query in this transaction
  // throws an error, the transaction will be aborted and the client released
  // back to the pool
  async query(...args) {
    log.debug('running query %j', args);
    if (!this._client) {
      throw new Error(`Cannot run query ${JSON.stringify(args)} on already closed transaction`);
    }
    this._lastQuery = args;
    return await this._client.query(...args);
  }

  // Mark a transaction as successful and release the client
  async success() {
    log.trace('committing transaction');
    if (!this._client) {
      throw new Error(`Cannot commit an already closed transaction`);
    }
    try {
      await this.query('COMMIT');
    } finally {
      this._client.release();
      clearTimeout(this._nag);
    }
    delete this._client;
  }

  // Mark a transaction as successful and release the client
  async failure(err) {
    if (!this._client) {
      throw new Error(`Cannot rollback an already closed transaction`);
    }
    try {
      await this.query('ROLLBACK');
    } finally {
      this._client.release();
      clearTimeout(this._nag);
    }
    delete this._client;
    if (err) {
      throw err;
    } else {
      throw new ErrRollback('query-rollback');
    }
  }
}
PG.SQL = SQL;

module.exports = {PG, SQL};
