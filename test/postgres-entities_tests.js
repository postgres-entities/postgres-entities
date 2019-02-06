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
          console.dir(err);
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
  });

  describe('field type tests', () => {
    function testField(clazz, {works=[], toDBFails=[], fromDBFails=[]}) {
      describe(clazz.name, () => {
        for (let work of works) {
          it(`should be true that '${work.toString()}' === toDB(fromDB('${work.toString()}'))`, () => {
            assume(clazz.fromDB(clazz.toDB(work))).deeply.equals(work);
          });
        }

        for (let toDBFail of toDBFails) {
          it(`input should fail for invalid toDB input '${toDBFail.toString()}'`, () => { 
            assume(() => {
              clazz.toDB(toDBFail);
            }).throws(/Field value/);
          });
        }

        for (let fromDBFail of fromDBFails) {
          it(`input should fail for invalid fromDB input '${fromDBFail.toString()}'`, () => { 
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
  });
});
