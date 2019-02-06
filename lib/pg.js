'use strict';

const log = require('./log')('pg');

const {EventEmitter} = require('events');
const {Pool, Client} = require('pg');
const {SQL} = require('sql-template-strings');

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
      console.log(`A transaction has been alive for 5s, last ran ${JSON.stringify(this.lastQuery)}`);
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
