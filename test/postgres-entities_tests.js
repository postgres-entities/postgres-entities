'use strict';

const assume = require('assume');
const tmp = require('tmp');
const rimraf = require('rimraf').sync;

const {PGTestDatabase} = require('../lib/pg-test-database');

const {
  PGEntityError,
  PGEntityManager,
  PGEntity,
  PGEntityDocument,
  PGStringField,
  PGIntegerField,
  PGBigIntField,
  PGDateField,
  PGTruthyFalsyField,
  PGBooleanField,
  PGJSONField,
  PGEntityQuery,
  VALID_JSON_FIELD,
  MAX_DOCUMENT_SIZE,
} = require('../lib/postgres-entities');

const {PG} = require('../lib/pg');

describe('Postgres Entities', () => {
  describe('database tests', () => {
    let datadir;
    let db; 

    before(async function() {
      this.timeout(10 * 1000);

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
      connopts.idleTimeoutMillis = 100;
    });

    after(async function() {
      this.timeout(10*1000);
      await db.stopPostgres();
    });

    describe('entity manager', () => {
      let subject;

      beforeEach(() => {
        subject = new PGEntityManager({
          service: 'test_service',
          connectionString: db.clientConnectionString(),
        });
      });

      afterEach(async () => {
        await subject.close();
      });

      it('should generate a valid postgres schema identifier', () => {
        assume(subject.psqlSchema).equals('"test_service"');
      });

      it('should generate valid postgres tables', async () => {
        let queries = subject.psqlCreateQueries;

        let client = await db.getSuperuserClient();
        try {
          for (let query of queries) {
            await client.query(query);
          }
        } finally {
          await client.end();
        }
      });

    });

    describe('entities', () => {
      let manager;
      let subject;

      // Just a helper to run the create queries since we'll need to run them
      // at different times
      async function runQueries(queries) {
        let client = await db.getSuperuserClient(true);
        try {
          for (let query of queries) {
            await client.query(query);
          }
        } finally {
          await client.end();
        }
      }

      beforeEach(async () => {
        manager = new PGEntityManager({
          service: 'phonebook',
          connectionString: db.clientConnectionString(),
        });

        await runQueries(manager.psqlDropQueries);
        await runQueries(manager.psqlCreateQueries);
      });

      afterEach(async () => {
        await manager.close();
      });

      it('should be possible to create a simple entity', async () => {
        let entity = new PGEntity({
          name: 'entity-1',
          id: 'name',
          manager,
          versions: [{
            fields: {
              name: PGStringField,
              feet: PGIntegerField,
              atoms: PGBigIntField,
              birthday: PGDateField,
            },
            indexes: ['name'],
          }],
        });

        // Should be able to run twice safely
        await runQueries(entity.psqlCreateQueries);
        await runQueries(entity.psqlCreateQueries);
      });

      it('should be possible to create a entity with multiple indexes', async () => {
        let entity = new PGEntity({
          name: 'entity-1',
          id: 'name',
          manager,
          versions: [{
            fields: {
              name: PGStringField,
              feet: PGIntegerField,
              atoms: PGBigIntField,
              birthday: PGDateField,
            },
            indexes: ['name', 'feet'],
          }],
        });

        // Should be able to run twice safely
        await runQueries(entity.psqlCreateQueries);
        await runQueries(entity.psqlCreateQueries);
      });

      it('should be possible to create a entity with a simple id', async () => {
        let entity = new PGEntity({
          name: 'entity-1',
          id: 'name',
          manager,
          versions: [{
            fields: {
              name: PGStringField,
              feet: PGIntegerField,
              atoms: PGBigIntField,
              birthday: PGDateField,
            },
            indexes: ['name', 'feet'],
          }],
        });

        // Object key
        assume(entity.calculateId({'name': 'John'}))
          .equals(Buffer.from('John').toString('base64'));
      });


      it('should be possible to create a entity with a composite id', async () => {
        let entity = new PGEntity({
          name: 'entity-1',
          id: ['name', 'feet'],
          manager,
          versions: [{
            fields: {
              name: PGStringField,
              feet: PGIntegerField,
              atoms: PGBigIntField,
              birthday: PGDateField,
            },
            indexes: ['name', 'feet'],
          }],
        });

        let name64 = Buffer.from('John').toString('base64');
        let feet64 = Buffer.from('2').toString('base64');

        assume(entity.calculateId({'name': 'John', feet: 2}))
          .equals(`${name64}_${feet64}`);

        assume(() => {entity.calculateId({'name': 'John'})})
          .throws(/Missing feet/);


      });

      it('should be possible to create, update and remove a single version entity', async () => {
        let entity = new PGEntity({
          name: 'entity-1',
          id: 'name',
          manager,
          versions: [{
            fields: {
              name: PGStringField,
              feet: PGIntegerField,
              atoms: PGBigIntField,
              birthday: PGDateField,
            },
            indexes: ['name', 'feet'],
          }],
        });

        await runQueries(entity.psqlCreateQueries);

        let document = entity.createDocument({
          value:{
            name: 'John',
            feet: 2,
            atoms: 7n * (27n ** 10n),
            birthday: new Date('2010-01-01'),
          }
        });

        await entity.insert(document);
        
        // Should collide if there's already a document stored there
        try {
          await entity.insert(document);
          return Promise.reject('should fail');
        } catch (err) {
          if (err.code !== 'PGEntityAlreadyExistsError') {
            throw err;
          }
        }

        await entity.load({name: 'John'});

        let loadedDocument = await entity.load(document);

        assume(loadedDocument).has.property('name', document.name);
        assume(loadedDocument).has.property('feet', document.feet);
        assume(loadedDocument).has.property('atoms', document.atoms);
        assume(loadedDocument).has.property('birthday');
        assume(loadedDocument.birthday.getTime()).equals(document.birthday.getTime());
        assume(loadedDocument).has.property('__etag');
        assume(loadedDocument).has.property('__version', 0);
        assume(loadedDocument).has.property('__fields');
        assume(loadedDocument).has.property('__lastModified');
        assume(loadedDocument).has.property('__sequence');

        // Returning the field values
        let updateEtag = await entity.update(document, doc => {
          doc.feet = 1;
          doc.atoms = 8n * (27n ** 10n);
          return document;
        });

        assume(updateEtag).is.ok();

        loadedDocument = await entity.load(document);
        assume(loadedDocument).has.property('feet', 1);
        assume(loadedDocument).has.property('atoms');
        assume(loadedDocument.atoms).equals(8n * (27n ** 10n));

        // Editing the document
        await entity.update(document, doc => {
          doc.feet = 2;
          doc.atoms = 9n * (27n ** 10n);
        });
        loadedDocument = await entity.load(document);
        assume(loadedDocument).has.property('feet', 2);
        assume(loadedDocument).has.property('atoms');
        assume(loadedDocument.atoms).equals(9n * (27n ** 10n));

        // Editing conflict
        let conflictingDocument = await entity.load(document);

        await entity.update(document, doc => {
          doc.feet = 1;
        });

        try {
          await entity.update(conflictingDocument, doc => {
            doc.feet = 1;
          });
          return Promise.reject('should fail');
        } catch (err) {
          if (!/Entity changed during the modification/.test(err.message)) {
            throw err;
          }
        }

        let noUpdateEtag = await entity.update(document, doc => {
          doc.feet = 2;
          doc.atoms = 8n * (27n ** 10n);
          return document;
        }, {updateEtag: false});
        assume(noUpdateEtag).is.not.ok();

        let metadata = await entity.metadata(document);

        document = await entity.load(document);
        assume(metadata).has.property('etag', document.__etag);

        await entity.remove(document);

        await entity.remove(conflictingDocument, {unconditional: true});
      })

      it('should not be possible to exceed maximum document size', async () => {
        let entity = new PGEntity({
          name: 'entity-1',
          id: 'name',
          manager,
          versions: [{
            fields: {
              name: PGStringField,
            },
            indexes: ['name'],
          }],
        });

        let longString;

        // Calculate a document which has a serialization exactly one byte too
        // many.  There's 12 bytes of JSON encoding overhead in the wrapper:
        //    '{"name": ""}'.length = 12
        let document = entity.createDocument({
          value:{
            name: Buffer.alloc(MAX_DOCUMENT_SIZE - 11, '💩').toString(),
          }
        });

        try {
          await entity.insert(document);
          return Promise.reject(new Error('should fail'));
        } catch (err) {
          if (!/Document is too large/.test(err.message)) {
            throw err;
          }
        }
      });

      it('should support migrations', async () => {
        let entity = new PGEntity({
          name: 'entity-1',
          id: 'name',
          manager,
          versions: [{
            fields: {
              name: PGStringField,
              feet: PGIntegerField,
              atoms: PGBigIntField,
              birthday: PGDateField,
            },
            indexes: ['name', 'feet'],
          }],
        });

        await runQueries(entity.psqlCreateQueries);

        // Insert a document at version 0 (first) that we'll migrate to version
        // 1 once we've added it
        let document = entity.createDocument({
          value:{
            name: 'John',
            feet: 2,
            atoms: 7n * (27n ** 10n),
            birthday: new Date('2010-01-01'),
          }
        });

        await entity.insert(document);

        // This version adds a hands property with a default value of 2
        entity.addVersion({
          fields: {
            name: PGStringField,
            feet: PGIntegerField,
            hands: PGIntegerField,
            atoms: PGBigIntField,
            birthday: PGDateField,
          },
          indexes: ['name', 'feet', 'hands'],
          migrate: fieldValues => {
            fieldValues.hands = 2;
            return fieldValues;
          },
        });

        // We want the new indexes
        await runQueries(entity.psqlCreateQueries);

        // Verify that loaded documents have the hands property
        // with the default value
        let loadedDocument = await entity.load({name: 'John'});
        assume(loadedDocument).has.property('hands', 2);

        // Now delete the first version so that we can verify that trying to do
        // a migration from a forgotten version works.  Note that since the
        // loaded and migrated version of the document was not persisted, it
        // can be reused for later tests
        entity._versions[0] = null;

        try {
          await entity.load({name: 'John'});
          return Promise.reject('should fail');
        } catch (err) {
          if (!/Entity version 0 is no longer understood/.test(err.message)) {
            throw err;
          }
        }

        // Persist the update to version 1
        await entity.update(loadedDocument);

        // Adding version 2 which will later be deleted once we've updated the
        // document to be version 2, so that we'll trip a too new error
        entity.addVersion({
          fields: {
            name: PGStringField,
            feet: PGIntegerField,
            hands: PGIntegerField,
            atoms: PGBigIntField,
            birthday: PGDateField,
          },
          indexes: ['name', 'feet', 'hands'],
          migrate: fieldValues => {
            fieldValues.hands = 2;
            return fieldValues;
          },
        });
        await runQueries(entity.psqlCreateQueries);

        // Persist the migration to version 2
        loadedDocument = await entity.load({name: 'John'});
        await entity.update(loadedDocument);

        // Now we get rid of version 2 since we don't want the PGEntity
        // to be able to understand the format
        entity._versions.pop();

        try {
          await entity.load({name: 'John'});
          return Promise.reject('should fail');
        } catch (err) {
          if (!/Entity version 2 is too new/.test(err.message)) {
            throw err;
          }
        }
      });
    });

    describe('document stream', () => {
      let manager;
      let entity;

      // Just a helper to run the create queries since we'll need to run them
      // at different times
      async function runQueries(queries) {
        let client = await db.getSuperuserClient(true);
        try {
          for (let query of queries) {
            await client.query(query);
          }
        } finally {
          await client.end();
        }
      }

      beforeEach(async () => {
        manager = new PGEntityManager({
          service: 'streaming',
          connectionString: db.clientConnectionString(),
        });

        entity = new PGEntity({
          name: 'streamed',
          id: 'field1',
          manager,
          versions: [{
            fields: {
              field1: PGStringField,
              field2: PGIntegerField,
              field3: PGIntegerField,
              field4: PGIntegerField,
              field5: PGStringField,
              datefield: PGDateField,
            },
            indexes: ['field1', 'field2', 'field3', 'field4'],
          }],
        });

        await runQueries(manager.psqlDropQueries);
        await runQueries(manager.psqlCreateQueries);
      });

      afterEach(async () => {
        await manager.close();
      });

      async function insertDocuments(num) {
        for (let i = 0; i < num; i++) {
          let e = entity.createDocument({
            value: {
              field1: 'field1_' + Number(i).toString(),
              field2: i,
              field3: i * 2,
              field4: i * 3,
              field5: 'hello',
              datefield: new Date(),
            }
          });

          await entity.insert(e);
        }
      }


      it('should be able to retreive all documents without conditions', async () => {
        await insertDocuments(20);

        let documents = [];

        await entity.documentStream(document => {
          documents.push(document);
          assume(document).has.property('field1');
          assume(document).has.property('field2');
          assume(document).has.property('field3');
          assume(document).has.property('field4');
          assume(document).has.property('datefield');
        });

        assume(documents).has.lengthOf(20);
      });

      it('should be able to retreive some documents with conditions', async () => {
        await insertDocuments(20);

        let documents = [];

        await entity.documentStream(document => {
          documents.push(document);
          assume(document).has.property('field1', 'field1_5');
          assume(document).has.property('field2');
          assume(document).has.property('field3');
          assume(document).has.property('field4');
          assume(document).has.property('datefield');
        }, {queryBuilder: query => {
          query.compare('field1', PGEntityQuery.comp.eq, 'field1_5'); 
        }});

        assume(documents).has.lengthOf(1);
      });
    });

    describe('pagination', () => {
      let manager;
      let entity;

      // Just a helper to run the create queries since we'll need to run them
      // at different times
      async function runQueries(queries) {
        let client = await db.getSuperuserClient(true);
        try {
          for (let query of queries) {
            await client.query(query);
          }
        } finally {
          await client.end();
        }
      }

      beforeEach(async () => {
        manager = new PGEntityManager({
          service: 'pagination',
          connectionString: db.clientConnectionString(),
        });

        entity = new PGEntity({
          name: 'paginated',
          id: 'field1',
          manager,
          versions: [{
            fields: {
              field1: PGStringField,
              field2: PGIntegerField,
              field3: PGIntegerField,
              field4: PGIntegerField,
              field5: PGStringField,
              datefield: PGDateField,
            },
            indexes: ['field1', 'field2', 'field3', 'field4'],
          }],
        });

        await runQueries(manager.psqlDropQueries);
        await runQueries(manager.psqlCreateQueries);
      });

      afterEach(async () => {
        await manager.close();
      });

      async function insertDocuments(num) {
        for (let i = 0; i < num; i++) {
          let e = entity.createDocument({
            value: {
              field1: 'field1_' + Number(i).toString(),
              field2: i,
              field3: i * 2,
              field4: i * 3,
              field5: 'hello',
              datefield: new Date(),
            }
          });

          await entity.insert(e);
        }
      }

      it('should be able to retreive all documents without conditions', async () => {
        await insertDocuments(20);

        let pageCount = 0;
        let docCount = 0;

        let continuationToken;

        do {
          let response = await entity.fetchPage({quantity: 2, continuationToken});

          continuationToken = response.continuationToken;

          pageCount++;
          docCount += response.documents.length;

          if (pageCount < 10) {
            assume(response.documents).has.lengthOf(2);
          }

          // Just here to make sure that logs don't get ruined by infinite page fetching
          assume(pageCount).atmost(11);
        } while (continuationToken);
 
        assume(pageCount).equals(11);
      });

      it('should be able to retreive all documents with conditions', async () => {
        await insertDocuments(20);

        let pageCount = 0;
        let docCount = 0;

        let continuationToken;

        do {
          let response = await entity.fetchPage({quantity: 2, continuationToken, queryBuilder: query => {
            query.compare('field2', PGEntityQuery.comp.gte, 10); 
          }});
            
          continuationToken = response.continuationToken;

          pageCount++;
          docCount += response.documents.length;

          if (pageCount < 5) {
            assume(response.documents).has.lengthOf(2);
          }

          // Just here to make sure that logs don't get ruined by infinite page fetching
          assume(pageCount).atmost(6);
        } while (continuationToken);
 
        assume(pageCount).equals(6);
      });


      it('should end pagination when the last page has as many items as are the limit');

    });
  });

  describe('pg queries', () => {
    let entity;
    let query;

    beforeEach(() => {
      entity = new PGEntity({
        name: 'entity-1',
        id: 'field1',
        versions: [{
          fields: {
            field1: PGStringField,
            field2: PGStringField,
            field3: PGStringField,
            field4: PGStringField,
            field5: PGStringField,
            boolfield: PGBooleanField,
            intfield: PGIntegerField,
            datefield: PGDateField,
          },
          indexes: ['field1', 'field2', 'datefield', 'intfield', 'boolfield'],
        }],
      });

      query = new PGEntityQuery(entity);
    });

    it('should generate a simple select statement for an entity', () => {
      assume(query.statement.text).equals('SELECT * FROM "public"."entity-1";');
    });

    it('should generate a select statement for a single string condition', () => {
      query.compare('field1', PGEntityQuery.comp.gt, 'value1');
      assume(query.statement.text).equals('SELECT * FROM "public"."entity-1" WHERE (value->>\'field1\')::text > $1;');
      assume(query.statement.values).deeply.equals(['value1']);
    });

    it('should generate a select statement for a single date condition', () => {
      let d = new Date();
      query.compare('datefield', PGEntityQuery.comp.gt, d);
      assume(query.statement.text).equals('SELECT * FROM "public"."entity-1" WHERE (value->>\'datefield\')::timestamptz > $1;');
      assume(query.statement.values).deeply.equals([d.toISOString()]);
    });

    it('should generate a select statement for a single integer condition', () => {
      query.compare('intfield', PGEntityQuery.comp.gt, 123);
      assume(query.statement.text).equals('SELECT * FROM "public"."entity-1" WHERE (value->>\'intfield\')::integer > $1;');
      assume(query.statement.values).deeply.equals([123]);
    });

    it('should generate a select statement for a single boolean condition', () => {
      query.compare('boolfield', true).and.compare('boolfield', false);
      assume(query.statement.text).equals('SELECT * FROM "public"."entity-1" WHERE (value->>\'boolfield\'::boolean IS TRUE AND (value->>\'boolfield\'::boolean IS FALSE;');
      assume(query.statement.values).deeply.equals([]);
    });

    it('should generate a select statement for multiple field conditions', () => {
      query
        .compare('field1', PGEntityQuery.comp.eq, 'value1')
        .and
        .compare('field2', PGEntityQuery.comp.eq, 'value2');

      assume(query.statement.text)
        .equals('SELECT * FROM "public"."entity-1" WHERE (value->>\'field1\')::text = $1 AND (value->>\'field2\')::text = $2;');
      assume(query.statement.values).deeply.equals(['value1', 'value2']);
    });

    it('should generate a select statement for nested field conditions', () => {
      query
        .paren
          .compare('field1', PGEntityQuery.comp.eq, 'value1')
          .and
          .compare('field2', PGEntityQuery.comp.eq, 'value2')
        .close;

      assume(query.statement.text)
        .equals('SELECT * FROM "public"."entity-1" WHERE ( (value->>\'field1\')::text = $1 AND (value->>\'field2\')::text = $2 );');
      assume(query.statement.values).deeply.equals(['value1', 'value2']);
    });

    it('should generate a select statement for time field conditions', () => {
      let value = new Date();

      query.compare('datefield', PGEntityQuery.comp.gt, value, 1, 'day')

      assume(query.statement.text)
        .equals('SELECT * FROM "public"."entity-1" WHERE (value->>\'datefield\')::timestamptz > $1 + interval \'1 day\';');
      assume(query.statement.values).deeply.equals([value.toISOString()]);
    
    });

    it('should generate a select statement for time field conditions relative to now', () => {
      let value = new Date();

      query.compare('datefield', PGEntityQuery.comp.gt, PGEntityQuery.NOW, 1, 'day')

      assume(query.statement.text)
        .equals('SELECT * FROM "public"."entity-1" WHERE (value->>\'datefield\')::timestamptz > now() + interval \'1 day\';');
      assume(query.statement.values).deeply.equals([]);
    
    });

    it('should generate a select statement for non-relative time comparisons', () => {
      let value = new Date();

      query.compare('datefield', PGEntityQuery.comp.gt, PGEntityQuery.NOW)

      assume(query.statement.text)
        .equals('SELECT * FROM "public"."entity-1" WHERE (value->>\'datefield\')::timestamptz > now();');
      assume(query.statement.values).deeply.equals([]);
    }); 

    it('should generate a select statement for sequence conditions', () => {
      query.compareColumn('sequence', PGEntityQuery.comp.gt, 1)

      assume(query.statement.text)
        .equals('SELECT * FROM "public"."entity-1" WHERE "sequence" > $1;');
      assume(query.statement.values).deeply.equals([1]);
    });

    it('should generate a select statement for touched conditions', () => {
      let value = new Date();

      query.compareColumnTime('touched', PGEntityQuery.comp.lte, value);

      assume(query.statement.text)
        .equals('SELECT * FROM "public"."entity-1" WHERE "touched" <= $1::timestamptz;');
      assume(query.statement.values).deeply.equals([value]);
    });

    it('should fail when and is not supported', () => {
      assume(() => {
        query.and;
      }).throws(/Invalid context to use .and/);
    }); 

    it('should fail when or is not supported', () => {
      assume(() => {
        query.or;
      }).throws(/Invalid context to use .or/);
    }); 

    it('should fail when closeParen is not supported', () => {
      assume(() => {
        query.close;
      }).throws(/Invalid context to use .close/);
    }); 

    it('should fail for too many opening parentheses', () => {
      query.paren.compare('field1', PGEntityQuery.comp.gt, 'value1');
      assume(() => {
        query.statement;
      }).throws(/Cannot generate query with mismatched parentheses/);
    });
  
    it('should fail for too many closing parentheses', () => {
      assume(() => {
        query.compare('field2', PGEntityQuery.comp.gt, 'value2').close;
      }).throws(/Invalid context to use .close/);
    });

    it('should fail when non-indexed fields are queried', () => {
      assume(() => {
        query.compare('field3', PGEntityQuery.comp.gt, 'value1');
      }).throws(/Cannot set condition for unindexed field/);
    });

    it('should be able to set a limit', () => {
      query.limit(10);
      assume(query.statement.text).equals('SELECT * FROM "public"."entity-1" LIMIT $1;');
      assume(query.statement.values).deeply.equals(['10']);

      query.limit(20);
      assume(query.statement.text).equals('SELECT * FROM "public"."entity-1" LIMIT $1;');
      assume(query.statement.values).deeply.equals(['20']);
    });

    it('should be able to set ordering conditions', () => {
      query.orderBySequence();
      assume(query.statement.text).equals('SELECT * FROM "public"."entity-1" ORDER BY sequence ASC;');
      assume(query.statement.values).deeply.equals([]);

      query.orderByReverseSequence();
      assume(query.statement.text).equals('SELECT * FROM "public"."entity-1" ORDER BY sequence DESC;');
      assume(query.statement.values).deeply.equals([]);

      query.orderByNewestModification();
      assume(query.statement.text).equals('SELECT * FROM "public"."entity-1" ORDER BY touched DESC;');
      assume(query.statement.values).deeply.equals([]);

      query.orderByOldestModification();
      assume(query.statement.text).equals('SELECT * FROM "public"."entity-1" ORDER BY touched ASC;');
      assume(query.statement.values).deeply.equals([]);
    });
  });

  describe('field type tests', () => {
    function testField(clazz, {constArgs=[], works=[], toDBFails=[], fromDBFails=[]}) {
      describe(clazz.name, () => {
        for (let work of works) {
          it(`should be true that '${String(work)}' === toDB(fromDB('${String(work)}'))`, () => {
            assume(clazz.fromDB(clazz.toDB(work))).deeply.equals(work);
          });

          it(`should be true that '${String(work)}' === toDB(fromDB('${String(work)}')) (instance)`, () => {
            let fieldInstance = new clazz(...constArgs);
            assume(fieldInstance.fromDB(fieldInstance.toDB(work))).deeply.equals(work);
          });
        }

        for (let toDBFail of toDBFails) {
          it(`input should fail for invalid toDB input '${String(toDBFail)}'`, () => { 
            assume(() => {
              clazz.toDB(toDBFail);
            }).throws(/Field value/);
          });
        }

        for (let fromDBFail of fromDBFails) {
          it(`input should fail for invalid fromDB input '${String(fromDBFail)}'`, () => { 
            assume(() => {
              clazz.fromDB(fromDBFail);
            }).throws(/Field value/);
          });
        }

      });
    }

    testField(PGStringField, {
      works: ['john'],
      toDBFails: [new Date(), [1, 2, 3]],
    });

    testField(PGIntegerField, {
      works: [-Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER, -1, 0, 1],
      toDBFails: [new Date(), [1, 2, 3]],
    });

    testField(PGBigIntField, {
      works: [0n, 10000000000000000000000000000000000000000000000000000000000000000n],
      toDBFails: [0, '1', {}],
      fromDBFails: ['abc', {}],
    });

    testField(PGDateField, {
      works: [new Date()],
      toDBFails: ['lkadsjl', new Date().toISOString()],
      fromDBFails: ['lalal'],
    });

    testField(PGTruthyFalsyField, {
      works: [true, false],
    });

    testField(PGBooleanField, {
      works: [true, false],
      toDBFails: [undefined, null, NaN, 'true', new Promise(() => {})],
    });

    testField(PGJSONField, {
      works: [{}, [], {a:1}, {a:{b:1}}],
    });

    // The JSON field is a little more complicated than other fields
    describe('JSON fields', () => {
      it('validation function when VALID_JSON_FIELD is return', () => {
        let field = new PGJSONField({validate: () => {return VALID_JSON_FIELD}});
        assume(field.toDB('john')).deeply.equals('john');
      });

      it('validation function should throw if VALID_JSON_FIELD is not return', () => {
        let field = new PGJSONField({validate: () => {return true}});
        assume(() => {
          field.toDB('john');
        }).throws(/Validation return value was unexpected/);
      });
    });
  });
});
