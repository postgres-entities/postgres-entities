'use strict';

const cluster = require('cluster');
const numCPUs = require('os').cpus().length;
const uuid = require('uuid');
const http = require('http');

const baseUrl = {
  hostname: process.env.SVC_HOSTNAME || '127.0.0.1',
  port: process.env.SVC_PORT || 5555,
}

async function request({method='GET', id, body}) {
  return new Promise((resolve, reject) => {
    let request = http.request(Object.assign({path: '/todo/' + id, method}, baseUrl), res => {
      let body = [];
      res.once('error', reject);
      res.on('data', chunk => {
        body.push(chunk);
      });
      res.on('end', () => {
        body = Buffer.concat(body);
        if (res.statusCode >= 300) {
          return reject(body.toString());
        }
        return resolve(body);
      });
    });
    request.once('error', reject);
    request.setHeader('content-type', 'application/json');
    if (body) {
      request.write(body);
    }
    request.end();
  });
}

async function test() {
  let n = 10;

  while (true) {
    let start = process.hrtime.bigint();
    for (let i = 0; i < n; i++) {
      let id = uuid.v4();

      await request({
        id,
        method: 'PUT',
        body: JSON.stringify({
          todoId: id,
          priority: Math.floor(Math.random() * 10) + 1,
          title: 'clean kitchen',
          body: 'make sure the kitchen is clean',
          due: new Date(),
        }),
      });

      await request({id});
      await request({id});

      await request({
        id,
        method: 'POST',
        body: JSON.stringify({
          todoId: id,
          priority: Math.floor(Math.random() * 10) + 1,
          title: 'dirty kitchen',
          body: 'make sure the kitchen is dirty',
          due: new Date(),
        }),
      });

      await request({id});
      await request({id});
      await request({id});

      if (Math.random() < 0.1) {
        await request({id, method: 'DELETE'});
      }
    }
    let end = process.hrtime.bigint();
    let duration = Number(end - start) / 1e9;
    console.log(`PID:${process.pid} generated load of ${n/duration} req/s`);
  };
}

if (cluster.isMaster) {
  console.log(`Master ${process.pid} is running`);

  // Fork workers.
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
    console.log(`started worker`);
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`worker ${worker.process.pid} died`);
    cluster.fork();
  });
} else {
  test().then(console.log, console.error);
}
