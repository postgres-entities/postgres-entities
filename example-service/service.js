'use strict';
const express = require('express');
const bodyParser = require('body-parser');
const debug = require('debug')('example-service');
const cluster = require('cluster');
const numCPUs = require('os').cpus().length;

const app = express()
const port = process.env.PORT || 5555;


const {
  PGEntityManager,
  PGEntity,
  PGEntityDocument,
  PGIntegerField,
  PGStringField, 
  PGDateField,
} = require('../');

const {setup} = require('./init-service');

const PGDATA = process.env.PGDATA || 'example-service-pgdir';
const PGPORT = '5454';


async function main() {

  // Determine which connection string to use
  let connectionString = await setup();

  // Each service needs an Entity Manager
  const entityManager = new PGEntityManager({
    service: 'todo-list',
    connectionString,
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

  try {
    // Ensure the schema is created
    await entityManager.write.runInTx(async tx => {
      for (let query of entityManager.psqlCreateQueries) {
        await tx.query(query);
      }
    });
  } catch (err) {
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

if (cluster.isMaster) {
  console.log(`Master ${process.pid} is running`);

  // Fork workers.
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`worker ${worker.process.pid} died`);
    cluster.fork();
  });
} else {
  main().then(() => {
    console.log(`Worker ${process.pid} started`);
  }, console.error);
}
