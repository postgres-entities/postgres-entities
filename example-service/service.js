'use strict';
const express = require('express');
const bodyParser = require('body-parser');
const fs = require('fs');
const debug = require('debug')('example-service');

const app = express()
const port = process.env.PORT || 5555;

const {
  PGEntityManager,
  PGEntity,
  PGEntityDocument,
  PGIntegerField,
  PGStringField, 
  PGDateField,
  PGTestDatabase,
} = require('../');

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

// Each service needs an Entity Manager
const entityManager = new PGEntityManager({
  service: 'todo-list',
  connectionString: process.env.DATABASE_URL || testDB.clientConnectionString(),
});

// Define the structure of a Todo item entity
const todoItem = new PGEntity({
  name: 'todo-item',
  id: ['todoId'],
  versions: [{
    fields: {
      'todoId': PGStringField,
      'priority': PGIntegerField,
      'title': PGStringField,
      'body': PGStringField,
      'due': PGDateField,
    },
  }],
  manager: entityManager,
});

async function main() {

  if (!process.env.DATABASE_URL) {
    // Only create the database if it doesn't already exist
    if (!fs.existsSync(testDB.datadir)) {
      // Initialise and start the Postgres databsae
      await testDB.configurePostgresDatadir();
    } 

    // Start and initialise the database.  For this simple example
    // service, we start with a fresh state each execution
    await testDB.startPostgres();
    await testDB.initialiseDatabase();


    // Create all of the Postgres objects required for this example
    let superClient = await testDB.getSuperuserClient(true);
    for (let query of entityManager.psqlCreateQueries) {
      await superClient.query(query);
    }
    await superClient.end();
  }

  // All bodies should be treated as JSON documents
  app.use(bodyParser.json({type: "*/*"}));

  // Define the Rest API
  app.get('/', (req, res) => {
    res.send('Hello, world');
  });

  app.get('/todo/:id', async (req, res) => {
    try {
      let id = req.params.id;
      debug('Loading ' + id);
      
      return res.send(JSON.stringify(await todoItem.load(id)));
    } catch (err) {
      console.dir(err);
      if (err.code === 'PGEntityNotFoundError') {
        return res.status(400).end();
      }
      return res.status(500).end();
    }
  });

  app.put('/todo/:id', async (req, res) => {
    try {
      let id = req.params.id;
      debug('Creating ' + id);

      let payload = req.body;

      if (payload.todoId !== id) {
        return res.status(400).end();
      }

      // Create a document and then insert it
      let document = todoItem.createDocument({
        value: {
          todoId: id,
          priority: payload.priority,
          title: payload.title,
          body: payload.body,
          due: new Date(payload.due),
        }
      });

      // TODO Demonstrate eventual consistency
      await todoItem.insert(document);
      res.status(200).end();
    } catch (err) {
      console.dir(err);
      if (err.code === 'PGEntityAlreadyExistsError') {
        return res.status(400).end();
      }
      return res.status(500).end();
    }
  });


  app.post('/todo/:id', async (req, res) => {
    try {
      let id = req.params.id;
      debug('Updating ' + id);

      let payload = req.body;

      if (payload.todoId !== id) {
        return res.status(400).end();
      }

      // Create a document and then insert it
      let document = await todoItem.load(id);

      await todoItem.update(document, () => {
        document.todoId = payload.todoId;
        document.priority = payload.priority;
        document.title = payload.title;
        document.body = payload.body;
        document.due = new Date(payload.due);
      });
      res.status(200).end();
    } catch (err) {
      console.dir(err);
      if (err.code === 'PGEntityNotFoundError') {
        return res.status(400).end();
      }
      return res.status(500).end();
    }
  });

  app.delete('/todo/:id', async (req, res) => {
    try {
      let id = req.params.id;
      debug('Removing ' + id);
      let document = await todoItem.load(id)
      await todoItem.remove(document);
      res.status(200).end();
    } catch (err) {
      if (err.code === 'PGEntityNotFoundError') {
        return res.status(400).end();
      }
      return res.status(500).end();
    }
  });

  await new Promise((resolve, reject) => {
    app.listen(port, err => {
      if (err) {
        reject(err);
      }
      debug(`Listening on port ${port}`);
      resolve();
    });
  });
}

main().catch(console.error);
