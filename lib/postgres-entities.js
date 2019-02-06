'use strict';
const log = require('./log')('entities');

const crypto = require('crypto');
const {parse: parseConnstr} = require('pg-connection-string');
const {PG, SQL} = require('./pg');

const VALID_SERVICE_NAME = /^[0-9A-Za-z_-]{3,20}$/
const VALID_ENTITY_NAME = /^[0-9A-Za-z_-]{3,20}$/
const VALID_FIELD_NAME = /^[0-9A-Za-z_-]{3,30}$/

// TODO Major things which are still to implement:
// 1. searching
// 2. joining
// 3. queueing

/**
 * Find exactly one result in the result of a query and throw an appropriate
 * error for other cases
 */
function _one(result) {
  switch(result.rowCount){
    case 1:
      return result.rows[0];
    case 0:
      throw new PGEntityNotFoundError('Entity not found');
    default:
      throw new PGEntityConsistencyError(
        `Expected 0 or 1 results but got ${result.rowCount}`);
  }
}

/**
 * Base class for all errors thrown from this class
 */
class PGEntityError extends Error {
  constructor(msg, entity) {
    super(msg);
    if (entity) {
      this.entityVersion = entity.version;
      this.entityName = entity.constructor.name;
    }
    log.error(msg);
    this.code = this.constructor.name;
  }
};

class PGPostgresError extends PGEntityError {
  constructor(pgErr, entity) {
    super(pgErr.message, entity);
    Object.assign(this, pgErr);
    this.pgStack = pgErr.stack;
  }
};

// TODO Reconsider which of these errors should really be here and make sure
// the appropriate errors are being thrown in the various places
class PGEntityNotFoundError extends PGEntityError {};
class PGEntityAlreadyExistsError extends PGEntityError {};
class PGEntityCollisionError extends PGEntityError {};
class PGEntityConsistencyError extends PGEntityError {};
class PGEntityTooNewError extends PGEntityError {};
class PGEntityTooOldError extends PGEntityError {};
class PGEntityTooLarge extends PGEntityError {};
class PGEntityInvalidMigrationError extends PGEntityError {};
class PGEntityInvalidDocumentError extends PGEntityError {};
class PGEntityInvalidFieldError extends PGEntityError {};
class PGEntityIDError extends PGEntityError {};
class PGEntityNonIndexableError extends PGEntityError {};

// TODO Decide how things like max/min clients will be passed.  The parsing
// library supports setting connection options as query string parameters but
// the connection strings from various hosts may not set them

class PGEntityManager {
  constructor({
    // Name of the service -- e.g. 'queue'
    service,
    // Postgres connection strings
    // TODO Rename
    connectionString,
    readConnectionString,
  }) {

    if (typeof service !== 'string' || !VALID_SERVICE_NAME.test(service)) {
      throw new Error('PGEntityManager must know which service it is working for');
    }
    this.service = service;

    if (typeof connectionString !== 'string') {
      throw new Error('Connection String is required');
    }

    this.connectionString = connectionString;
    this._write = new PG(parseConnstr(connectionString));

    if (readConnectionString) {
      this.readConnectionString = readConnectionString;
      this._read = new PG(parseConnstr(readConnectionString));
    } else {
      this._read = this._write;  
    }

    this._entities = {};
  }

  /**
   * Close the managed pools
   */
  async close() {
    await this._write.close();
    if (this._read !== this._write) {
      await this._read.close();
    }
  }

  addEntity(entity) {
    this._entities[entity.name] = entity;
  }

  // Implemented as a read only getter to avoid accidental assignments
  get read() {
    return this._read;
  }

  // Implemented as a read only getter to avoid accidental assignments
  get write() {
    return this._write;
  }

  // Return the name of the schema for this service
  get psqlSchema() {
    return `${PG.escapeIdentifier(this.service)}`;
  }

  // Create global db objects
  get psqlCreateQueries() {
    let queries = [];

    let {username} = new URL(this.connectionString);

    // Each service will have its own schema.  This allows seperation of
    // services within a single database.
    queries.push(`CREATE SCHEMA ${this.psqlSchema};`);
    queries.push(`GRANT USAGE ON SCHEMA ${this.psqlSchema} TO ${PG.escapeIdentifier(username)}`);

    // PGCrypto is distributed with Postgres and is used here to generate
    // UUIDs which are used as the etag for an entity.  This extension is
    // fairly commonly found.
    //   1. Fedora package 'postgres-contrib'
    //   2. Debian package 'postgres-contrib...'
    //   3. Google Cloud Sql -- https://cloud.google.com/sql/docs/postgres/extensions#misc
    //   4. Amazon RDS -- https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_PostgreSQL.html#PostgreSQL.Concepts.General.FeatureSupport.Extensions.101x
    //   5. Azure Postgres -- https://docs.microsoft.com/en-us/azure/postgresql/concepts-extensions
    //   5. Heroku -- https://devcenter.heroku.com/articles/heroku-postgres-extensions-postgis-full-text-search#functions
    queries.push(`CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA ${this.psqlSchema};`);

    // This function is defined for all entities inside of this schema.  It is
    // used to ensure that the touched time (time of last modification) is up
    // to date, as well as generating a new etag each time the entity is
    // modified.  This should allow serving etags from our database which are
    // accurate, which would be nice as it would allow use of If-Match and
    // If-None-Match headers.  It is also general enough that every entity would
    // automatically have this etag/last-modified support.
    queries.push([
      `CREATE OR REPLACE FUNCTION ${this.psqlSchema}.update_entity()`,
      'RETURNS TRIGGER AS $$',
      'BEGIN',
      '  IF row(NEW.*) IS DISTINCT FROM row(OLD.*) THEN ',
      '    NEW.touched = now();',
      `    NEW.etag = ${this.psqlSchema}.gen_random_uuid();`,
      '    RETURN NEW;',
      '  ELSE ',
      '    RETURN OLD;',
      '  END IF;',
      'END;',
      '$$ language \'plpgsql\';',
    ].map(x => x.trim()).join(' '));

    for (let entity in this._entities) {
      Array.prototype.push.apply(queries, this._entities[entity].psqlCreateQueries);
    }

    return queries.map(query => {
      if (typeof query === 'string') {
        return {text: query};
      } else {
        return {text: query.text, values: query.values};
      }
    });
  }

  get psqlDropQueries() {
    let queries = [];

    queries.push(`DROP SCHEMA IF EXISTS ${this.psqlSchema} CASCADE;`);

    return queries;
  }
}

/**
 * PGEntity represents a collection of PGEntityDocuments.  These are both
 * terrible names and should be changed.  A PGEntity understands what the
 * structure of each document is, how to create, load, store, update and remove
 * each document.
 *
 * The basic idea is that a PGEntity represents the idea of a Table in a
 * database and each PGEntityDocument represents a row.
 *
 * Each PGEntity has a table with the following columns:
 *   1. id -- text
 *   2. value -- jsonb, containing the actual document
 *   3. etag -- uuid, automatically updated by postgres on edit
 *   4. touched -- timestamptz, automatically updated by postgres on edit
 *  
 * While the table format is relatively fixed, the format of the data stored
 * within is designed to be easy to migrated.  The only thing which cannot
 * change is the shape of the id field.  Each change of the format of the data
 * stored is called a version.  Each version contains the information which
 * describes the format of the properties on the PGEntityDocument, those
 * columns which are indexable and for the second and later versions a
 * migration function.
 *
 * One reason for the particular etag and touched field being updated inside
 * the database automatically is the desire to allow these to be served as actual
 * http etags and last-modified values.  This would help with caching and allow
 * skipping operations where unneeded.
 *
 * The in-database identifier is computed based on the value of a
 * PGEntityDocument and must be unique as it serves as the primary key for the
 * table.
 *
 * The value stored in the database is stored as JSONB column.  The structure
 * of the value is defined by a list of fields which themselves describe the
 * acceptable formats which can be stored there.  In essence, a document is a
 * normal JS Object which can have functions and values added to it, but when
 * passed to a PGEntity function will have those properties which are tracked
 * by fields to be persisted in the database.
 *
 * Think of an object {a: 1, b: 'john', c: new Date(), d: {e:1}}.  If that
 * object were persisted by a PGEntity, the property awould be an integer
 * field, b would be string, c would be a string containing an ISO8601 UTC
 * timestamp, d would be an object.  Indexes would be constructed for a, b and
 * c using the `->>` operator of Postgres's JSON functions.  Once support is
 * added, it will be possible to search for documents which have a specific 'a'
 * value or even do various joins.
 * 
 * This object has getters which start with `psql` and are used only for
 * generating sql or sql fragments.  They are not generally useful
 *
 * This class is not intended to be subclassed for general usage.
 */
class PGEntity {
  constructor({
    // Name of the PGEntity instance -- e.g. 'tasks'.  This field is restricted
    // to 0-9, A-Z, a-z - and _ as it is not user facing.
    name,
    // This is a string or list of strings which are field names which are used
    // to compute the database identifier.  See this.prototype.calculateId for
    // more information on the format
    id, // TODO consider renaming idField
    // Allow setting versions in the constructor -- see this.prototype.addVersion
    // for more information on the format
    versions = [],
    // A reference to the manager for this PGEntity instance
    manager,
    // A constructor which is a subclass of the PGEntityDocument and presents
    // the same interface.  This is done to make it possible to add custom
    // behaviours to documents.  The idea is that *most* documents will not
    // need any special methods
    DocumentConstructor = PGEntityDocument,
  }) {
    if (typeof name !== 'string' || !VALID_ENTITY_NAME.test(name)) {
      throw new Error('Name must be valid');
    }
    this.name = name;

    // TODO Figure out if we should have a context option here which can be
    // bound to each Document which is created... This would allow 'passing' in
    // configuration things into possible Document subclass functions without
    // having to actually persist them.  This object should be static per
    // pgentity instance and should be assigned to the .__context proeprty on
    // the Document

    // The id parameter is a string or list of strings that make up the
    // database id.  This value can never change through the various
    // migrations, so it is set at the entity level and must be checked to
    // still exist in each version of the entity.  It is stored in the object
    // as a list in all cases to simplify later usage
    if (typeof id === 'string') {
      this._id = [id];
    } else if (Array.isArray(id) && id.filter(x => typeof x !== 'string').length === 0) {
      this._id = id;
    } else {
      throw new PGEntityInvalidFieldError('Id value is not a string or list of strings');
    }

    // Use the same logic to store versions regardless of whether they are
    // passed in or set after the constructor
    this._versions = [];
    if (versions && !Array.isArray(versions)) {
      throw new Error('Verions property must be an array of versions');
    }
    for (let version of versions) {
      this.addVersion(version);
    }
    
    // If a manager is set during the constructor, use the same logic to
    // register it as would be done after the constructor
    if (manager) {
      this.registerManager(manager);
    }

    this.DocumentConstructor = DocumentConstructor;
  }

  /**
   * Set the manager for this instance and tell that manager
   * about this entity
   */
  registerManager(manager) {
    manager.addEntity(this);
    this.manager = manager;
  }

  /**
   * Add a version to a PGEntity.  Each version must have a fields object and
   * indexes array.  For the second and later version, the migrate function
   * must be provided.  If the option is falsy instead of an object, a null
   * value will be added.  This is to allow old versions to be removed
   * after they are no longer needed.
   *
   * The migrate function will not be passed an instance of a document, but
   * rather an Object which contains the field values.
   */
  addVersion(options) {
    if (!options) {
      this._versions.push(null);
      return;
    }

    let {migrate, fields, indexes=[]} = options;

    if (typeof fields !== 'object') {
      throw new PGEntityInvalidFieldError('No fields specified');
    }

    // A migrate function is a sync function which is passed the JS version of the values
    // stored in this entity, and returns a new version of the fields.  There is no Document
    // created, rather it is operating directly on the low level field values.  This is a conscious
    // choice, so that we do not have to maintain versioned document constructors
    if (this._versions.length !== 0 && typeof migrate !== 'function') {
      throw new PGEntityInvalidFieldError('Migrate is not a function');
    }

    for (let field in fields) {
      if (!VALID_FIELD_NAME.test(field)) {
        throw new PGEntityInvalidFieldError(`Field ${field} is not a valid name`);
      }

      // Note that this checks for subclassing specifically, as a PGEntityField
      // is not really a valid field at all
      if (!fields[field].prototype instanceof PGEntityField) {
        throw new PGEntityInvalidFieldError(`Field ${field} is not a valid type`);
      }
    }

    for (let idField of this._id) {
      if (!fields[idField]) {
        throw new PGEntityInvalidFieldError(`ID field ${field} must be present in entity version`);
      }
      if (!fields[idField].id) {
        throw new PGEntityInvalidFieldError(`ID field ${idField} cannot be part of id`);
      }
    }

    let _indexes = [];
    
    // Verify that all indexes requested are indexable
    for (let index of indexes) {
      index = typeof index === 'string' ? [index] : index;
      for (let indexField of index) {
        if (!fields[indexField].indexable) {
          throw new PGEntityNonIndexableError(`Field ${field.name} cannot be included in an index`);
        }
      }
      
      _indexes.push(index)
    }

    this._versions.push({migrate, fields, indexes: _indexes});

    // TODO Maybe we should verify when adding a new version that all defined versions of
    // the entity have the same types for the id fields and throw an error if not.
  }

  /**
   * Get the current version number.  All migrations occur on load, so there's
   * never a chance to use an outdated document
   */
  get currentVersionNumber() {
    return this._versions.length - 1;
  }

  /**
   * Return the current version
   */
  get currentVersion() {
    return this._versions[this.currentVersionNumber];
  }

  /**
   * Perform all needed migrations.  Each migration function is passed an object
   * with the field values as properties and not a full PGEntityDocument
   */
  migrate(fieldValues, storedVersion) {
    if (storedVersion > this.currentVersionNumber) {
      let msg = `Entity is too new (${storedVersion} > ${this.currentVersionNumber})`;
      throw new PGEntityTooNewError(msg);
    }
    
    for (let version of this._versions.slice(storedVersion + 1, this._versions.length)) {
      if (!version) {
        throw new PGEntityTooOldError(`Entity version ${storedVersion} is no longer understood`);
      }

      fieldValues = version.migrate(fieldValues);
    }

    return fieldValues;
  }


  /**
   * Based on a string, field value fragment, complete set of field values
   * or PGEntityDocument, determine what it's database id is.
   *
   * The format for keys is the base64 encoded version of the JS value's string
   * representation, with composite keys being joined with an underscore.
   *
   * In the case that the value is passed in as a string and the id has a single
   * field named, interpret that value as the id.
   *
   * The passed values must be Document values (i.e. result of Field.fromDB),
   * but the key will encoded using the database values (i.e. the result of
   * Field.toDB)
   */
  calculateId(value) {
    let fields = this.currentVersion.fields;
    let idValues = [];

    for (let idField of this._id) {
      if (!value[idField]) {
        throw new PGEntityIDError(`Missing ${idField} when calculating id`);
      }
      idValues.push(fields[idField].toDB(value[idField]).toString());
    }

    return idValues.map(x => Buffer.from(x).toString('base64')).join('_');
  }

  /**
   * Construct a new document, either when loading from the database or when
   * creating a fresh document.
   */
  createDocument({value, etag, version}) {
    if (!value) {
      throw new PGEntityInvalidDocumentError('Document must have value');
    }
    // make sure to store __etag, __persistedProperties
    let document = new this.DocumentConstructor({...value});
    document.__fields = this.currentVersion.fields;
    document.__version = version;

    if (etag) {
      document.__etag = etag;
    }

    return document;
  }

  /**
   * Given the JS values returned from the database, create and return a document
   */
  fromDB ({value, etag, version}) {
    if (typeof version !== 'number' || typeof value !== 'object') {
      throw new Error('Write a better error here');
    }

    if (version > this.currentVersionNumber) {
      throw new PGEntityTooNewError(`Entity version ${version} is too new`);
    }

    let dbVersion = this._versions[version];
    if (!dbVersion) {
        throw new PGEntityTooOldError(`Entity version ${version} is no longer understood`);
    }

    let fieldValues = {};

    for (let fieldName of Object.keys(dbVersion.fields)) {
      if (!value[fieldName]) {
        throw new PGEntityInvalidDocumentError(`Missing field ${fieldName}`);
      }

      fieldValues[fieldName] = dbVersion.fields[fieldName].fromDB(value[fieldName]);

    }

    if (version !== this.currentVersionNumber) {
      fieldValues = this.migrate(fieldValues, version);
    }

    return this.createDocument({value: fieldValues, etag, version});
  }

  /**
   * Given a document, return the JS values which can be JSON-stringify'd for storing
   * in the database.  This returns an object, not a json string.
   */
  toDB (obj) {
    if (!obj) {
      throw new PGEntityInvalidDocumentError('Refusing to serialise a non-object');
    }

    let psqlValue = {};

    for (let fieldName of Object.keys(this.currentVersion.fields)) {
      if (!obj[fieldName]) {
        throw new PGEntityInvalidDocumentError(`Missing field ${fieldName}`);
      }

      psqlValue[fieldName] = this.currentVersion.fields[fieldName].toDB(obj[fieldName])
    }

    return {
      etag: obj.__etag,
      id: this.calculateId(obj),
      value: psqlValue,
      version: this.currentVersion
    };
  }

  // TODO add touched into the mix for allowing it to be set as last-modified header values

  /**
   * Load document metadata from a id-calcuable object or string, performing
   * any migrations needed to update to the latest version of the fields
   */
  async metadata(from) {
    // TODO implement retries
    let id = this.calculateId(from);
    let {etag} = _one(await this.manager.read
      .query(SQL`SELECT etag FROM `
        .append(this.psqlTableName)
        .append(SQL` WHERE id = ${id};`)
      ));

    return {etag};
  }

  /**
   * Load a document from a id-calcuable object or string, performing any
   * migrations needed to update to the latest version of the fields
   */
  async load(from) {
    // TODO implement retries
    let id = this.calculateId(from);
    let {etag, value, version} = _one(await this.manager.read
      .query(SQL`SELECT etag, value, version FROM `
        .append(this.psqlTableName)
        .append(SQL` WHERE id = ${id};`)
      ));

    return this.fromDB({value, version, etag});
  }

  /**
   * Insert a document into the database.
   */
  async insert(document) {
    // TODO check for db-level collisions and throw right error
    // TODO implement retries

    // Do all the prep before we even open a transaction.  No need to talk to
    // the DB if the document isn't safe to serialise
    let documentId = this.calculateId(document);
    let {value: valueForDb, version} = this.toDB(document);
    // We are only including tracked fields.  This is in case the document
    // methods want to attach properties to document instances.
    let currentFields = Object.keys(this.currentVersion.fields);
    let jsonValue = JSON.stringify(valueForDb, currentFields);


    return await this.manager.write.runInTx(async tx => {
      try {
        await tx.query(SQL`INSERT INTO `.append(this.psqlTableName)
          .append(SQL` (id, value, version) VALUES (${documentId}, `)
          .append(SQL`${jsonValue}::jsonb, ${this.currentVersionNumber});`));
      } catch (err) {
        if (err.code === '23505') { // unique_violation
          let alreadyExistsErr = new PGEntityAlreadyExistsError('Entity Already exists');
          // TODO Consider getting the existing document and setting it as a
          // property on the error to aid in helping with writing idempotent
          // inserts
          throw alreadyExistsErr;
        }
        throw err;
      }

      let {etag} = _one(await tx.query(SQL`SELECT etag FROM `
          .append(this.psqlTableName).append(SQL` WHERE id = ${documentId}`)));

      document.__etag = etag;

      return document;
    });
  }

  /**
   * Update a document.  This is done with optimistic concurrency to avoid
   * locking the database row while other things happen.  First a document is loaded,
   * then things happen which require changed to the document.  Once those changes are completed,
   * this function is called to change the values in the database.  This function ensures that
   * no changes have occured on the value in the database without having to lock the whole database
   * table.
   *
   * TODO Decide if we want a locking version of updates which cannot fail because of changes
   * from other consumers... maybe?
   */
  async update(document, modify, options={updateEtag: true}) {
    // We're saving the original document's id so that if it changes while
    // editing the document we update the correct item.  Basically, we want to
    // do the optimistic concurrency lookup and the update using the original
    // id, but then the query to find the new etag with the new id
    //
    // The returnDocument property specifies whether to return a copy of the
    // document with an updated etag.  This is a performance optimization to
    // allow skipping a query to get the etag of the document.  If this value
    // is false, there is no document returned to help avoid errors.
    //
    // TODO: Decide if we want this or if we'd rather have the proxy in the
    // document block changes to the id properties, so that the id is immutable

    let originalId = this.calculateId(document);

    if (!document.__etag) {
      throw new PGEntityInvalidDocumentError('A document must have an etag to perform an update');
    }

    if (modify) {
      // Do the actual modification
      let _d = modify(document);
      if (_d) {
        if (typeof _d === 'object' && typeof _d.then) {
          _d = await _d;
        }
        document = _d;
      }
    }

    // Though, in most cases the id will never change, we want to support that
    let newId = this.calculateId(document);

    let {value: valueForDb, version} = this.toDB(document);

    // We are only including tracked fields.  This is in case the document
    // methods want to attach properties to document instances.
    let currentFields = Object.keys(this.currentVersion.fields);
    let jsonValue = JSON.stringify(valueForDb, currentFields);

    return await this.manager.write.runInTx(async tx => {
      let {etag: storedEtag} = _one(await tx.query(SQL`SELECT etag FROM `
        .append(this.psqlTableName)
        .append(SQL` WHERE id = ${originalId} FOR UPDATE`)));

      if (storedEtag !== document.__etag) {
        throw new PGEntityCollisionError(`Entity changed during the modification`);
      }

      await tx.query(SQL`UPDATE `
        .append(this.psqlTableName)
        .append(SQL` SET id = ${newId}, `)
        .append(SQL`value = ${jsonValue}::jsonb, `)
        .append(SQL`version = ${this.currentVersionNumber} `)
        .append(SQL` WHERE id = ${originalId};`));

      // TODO Handle the edit failing because of a collision and throw that error

      // We only want to lookup the new etag if requested.  Doing this lookup
      // is the default behaviour, but can be disabled
      if (options.updateEtag) { 
        let {etag: newEtag} = _one(await tx.query(SQL`SELECT etag FROM `
          .append(this.psqlTableName)
          .append(SQL` WHERE id = ${newId}`)));

        document.__etag = newEtag;
        return document;
      } 
    });
  }

  /**
   * Remove an item from the database if it hasn't been modified since being
   * loaded.  If the item is already absent in the database, the delete is
   * skipped.
   *
   * In any case where the document specified is no longer present in the
   * database, the document without its __etag property is returned.
   *
   * If the unconditional option is set, then the entity will be deleted
   * regardless of whether or not other operations have happened to the
   * document
   */
  async remove(document, options={unconditional: false}) {

    let id = this.calculateId(document);

    // Unconditional deletes need not occur in a transaction
    if (options.unconditional) {
      await this.manager.write.query(SQL`DELETE FROM `
        .append(this.psqlTableName)
        .append(SQL` WHERE id = ${id}`));
    } else {
      await this.manager.write.runInTx(async tx => {
        let result = await tx.query(SQL`SELECT etag FROM `
          .append(this.psqlTableName)
          .append(SQL` WHERE id = ${id} FOR UPDATE`));

        // If the entity does not exist, there's nothing to do here
        if (result.rowCount === 0) {
          return;
        } else if (result.rowCount !== 1) {
          throw new PGEntityConsistencyError(`Entity has consistency error`);
        }

        if (result.rows[0].etag !== document.__etag) {
          throw new PGEntityCollisionError(`Entity changed during removal`);
        }

        await tx.query(SQL`DELETE FROM `
          .append(this.psqlTableName)
          .append(SQL` WHERE id = ${id};`));
      });
    }

    delete document.__etag;
    return document;
  }

  /**
   * Forcibly remove a document from the database.  This is not safe because the
   * document maybe have been removed by another user in the interim.
   *
   * This method is mostly here to avoid hacks being written, but is absolutely
   * not recommended for production usage.
   */
  async forceRemove(id) {
    let id = this.calculateId(document);

    await this.manager.write.query(SQL`DELETE FROM `
      .append(this.psqlTableName)
      .append(SQL` WHERE id = ${id};`));

  }

  /**
   * Return PSQL safe view of the schema to use for database operations
   * on this PGEntity
   */
  get psqlSchema() {
    return this.manager.psqlSchema || '"public"';
  }

  /**
   * Return PSQL safe view of the table name (with schema) to use for database
   * operations on this PGEntity
   */
  get psqlTableName() {
    return `${this.psqlSchema}.${PG.escapeIdentifier(this.name)}`;
  }

  /**
   * Return PSQL safe view of queries needed to generate compatible Postgres
   * objects.  Note that this might not be the queries which generate
   * production databases, however, likely will serve as a starting point.
   * These serve as documentation and also aid in the unit testing of PGEntity
   * and PGEntityDocuments since this code is used in all unit tests
   */
  get psqlCreateQueries() {
    let queries = [];

    queries.push([
      'CREATE TABLE IF NOT EXISTS',
      this.psqlTableName,
      '(id TEXT PRIMARY KEY,',
      'value JSONB NOT NULL,',
      'version INTEGER NOT NULL,',
      `etag UUID DEFAULT ${this.psqlSchema}.gen_random_uuid(),`,
      'touched TIMESTAMPTZ NOT NULL DEFAULT NOW());',
    ].join(' '));

    let {username} = new URL(this.manager.connectionString);

    queries.push([
      // TODO figure out why this is failing
      'GRANT SELECT, INSERT, UPDATE, DELETE ON',
      this.psqlTableName,
      'TO',
      PG.escapeIdentifier(username),
      ';',
    ].join(' '));

    // Triggers have no create if not exists functionality, but DDL is
    // transactional, so this will drop and recreate it atomically
    queries.push('BEGIN');
    queries.push(`DROP TRIGGER IF EXISTS ${PG.escapeIdentifier('trg_upd_enty_' + this.name)} ON ${this.psqlTableName}`);
    queries.push([
      'CREATE TRIGGER',
      PG.escapeIdentifier('trg_upd_enty_' + this.name),
      'BEFORE UPDATE ON',
      this.psqlTableName,
      'FOR EACH ROW EXECUTE PROCEDURE',
      `${this.psqlSchema}.update_entity();`,
    ].join(' '));
    queries.push('COMMIT');
    
    for (let version of this._versions) {
      if (!version) {
        continue;
      }
      let {fields, indexes} = version;

      for (let index of indexes) {
        // Concurrently so that the app doesn't need to be stopped
        // If not exists so that different versions don't create duplicated
        // indexes
        // TODO: We might wish to have timeboxed indexes

        // The name is such that the index is content-addressable, meaning that
        // if the index for a specific section of the document
        // TODO We might wish to support altering indexes to only include
        // recenetly touched things -- e.g. only index entities which are touched
        // within the last 20 days.  This is likely to be an important performance concern
        queries.push([
          'CREATE INDEX CONCURRENTLY IF NOT EXISTS',
          PG.escapeIdentifier(`idx_${this.name}_${index.join('_')}`),
          `ON ${this.psqlTableName}`,
          `(${index.map(x => `(value ->> ${PG.escapeLiteral(x)})`).join(', ')});`,
        ].join(' '));
      }
    }
    return queries;
  }

  /**
   * Generate the PSQL queries needed to drop this PGEntity from the
   * database
   */
  get psqlDropQueries() {
    let queries = [];

    queries.push(SQL`DROP TABLE IF EXISTS `.append(this.psqlTableName).append(' CASCADE;'));

    return queries;
  }


}

/**
 * A Simple base class which can be managed by PGEntity, since
 * many implementations are unlike to need anything more advanced.
 * The interface for subclasses must remain compatible, since they
 * will be created by PGEntity.  The interface is:
 *
 *   1. Constructor must take a single object where each persisted field's
 *      value is given as a property on this one object.
 *   2. A property __etag is set for instances loaded from the database
 *      which contains the value of the etag when the document was last
 *      updated in the database.  This property must not be altered.
 *   3. A property __fields is frozen reference to the fields which for
 *      the entity which owns this document
 *
 * TODO decide if there should be a .__validate() function which does field
 * validation to ensure that the values can be PGEntityField.toDB'd
 */
class PGEntityDocument {
  constructor(...options) {
    Object.assign(this, ...options);
  }
}

/**
 * PGEntityField is a class which represents the understanding of how to map
 * PGEntityDocument property to and from a JS Value suitable for
 * JSON.stringify.  Each type of PGEntityField should be a class which extends
 * from PGEntityField.  A default toDB and fromDB implementation which does no
 * alterations is provided for those fields which do not need to perform any
 * translation.  
 *
 * Additionally, each field can have a .id and .indexable boolean field.  The
 * .id field is true if the field can form a part of the item's identifier and
 * the .indexable field is true if the field supports being indexed.  Only keys
 * which are indexable will be searchable when searching is implemented.
 *
 * TODO PGEntities should support instances of PGEntityFields to allow
 * fields which have configuration values which must be bound at runtime.  An
 * example would be a hypothetical PGEncryptedStringField which needs to be
 * configured with an encryption key at runtime
 *
 * TODO boolean and a non-indexable json field type
 */
class PGEntityField {}
PGEntityField.indexable = false; // Can this be indexable?
PGEntityField.id = false; // Can this be part of an id?
PGEntityField.toDB = function (value) {
  return value;
}
PGEntityField.fromDB = function (value) {
  return value;
}

/**
 * String
 */
class PGStringField extends PGEntityField {}
PGStringField.indexable = true;
PGStringField.id = true;
PGStringField.toDB = function(value) {
  if (typeof value !== 'string') {
    throw new PGEntityInvalidFieldError(`Field value is not a string`);
  }

  return value;
}

/**
 * Integer
 */
class PGIntegerField extends PGEntityField {}
PGIntegerField.indexable = true;
PGIntegerField.id = true;
PGIntegerField.toDB = function(value) {
  if (!Number.isInteger(value) || value > Number.MAX_SAFE_INTEGER) {
    throw new PGEntityInvalidFieldError(`Field value is not integer or exceeds maximum safe integer in javascript`);
  }

  return value;
}

/**
 * BigInt -- these are arbitrarily large integers as supported by Node 10 or
 * higher.  They need to be written out as strings because they can
 * theoretically be of any length.  This implementation serves as the most
 * interesting PGEntityField subclass implemented so far.  It converts the
 * bigint to a string in base-16 (hex) so that it can use fewer characters
 * along with the needed 0x prefix to allow easy reconstruction by the BigInt
 * function
 */
class PGBigIntField extends PGEntityField {}
PGBigIntField.indexable = true;
PGBigIntField.id = true;
PGBigIntField.toDB = function(value) {
  if (typeof value !== 'bigint') {
    throw new PGEntityInvalidFieldError(`Field value is not a big int`);
  }
  return '0x' + value.toString(16);
}

PGBigIntField.fromDB = function(value) {
  if (typeof value !== 'string') {
    throw new PGEntityInvalidFieldError(`Field value cannot convert to BigInt`);
  }

  try {
    return BigInt(value);
  } catch (err) {
    throw new PGEntityInvalidFieldError(`Field value cannot convert to BigInt`);
  }
}

/**
 * Dates
 */
class PGDateField extends PGEntityField {}
PGDateField.indexable = true;
PGDateField.toDB = function(value) {
  if (value.constructor.name !== 'Date') {
    throw new PGEntityInvalidFieldError(`Field value ${value} is not a Date object`);
  }
  return new Date(value).toISOString();
}

PGDateField.fromDB = function(value) {
  let x = Date.parse(value);
  if (x !== x) { // NaN check
    throw new PGEntityInvalidFieldError(`Field value ${value} is not a Date object`);
  }
  return new Date(value);
}

module.exports = {
  VALID_ENTITY_NAME,
  VALID_FIELD_NAME,
  // To avoid duplication, only the base Error will be exported.  Specialized
  // errors will have the correct code property, but the base error is useful
  // for instanceof checks
  PGEntityError,
  PGEntityManager,
  PGEntity,
  PGEntityDocument,
  PGStringField,
  PGIntegerField,
  PGBigIntField,
  PGDateField,
};
