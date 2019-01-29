'use strict';
const uuid = require('uuid');

const http = require('http');

function testHeader({msg}) {
  console.log(`PID:${process.pid} starting ${msg}`);
  return process.hrtime.bigint();
}

function testFooter({start, n, msg}) {
  let end = process.hrtime.bigint();
  let duration = Number(end - start) / 1e6;
  console.log(`PID:${process.pid} completed ${msg} ${n/duration} req/ms`);
}

async function request({method='GET', id, body}) {
  return new Promise((resolve, reject) => {
    let request = http.request({hostname: '127.0.0.1', port: 5555, path: '/todo/' + id, method}, res => {
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

async function main() {
  let concurrent = 25;
  let n = 1000 * concurrent;
  let completed = 0;
  let uuids = [];

  let start = testHeader({msg: 'put requests'});
  for (let i = 0 ; i < n; i += concurrent) {
    let p = [];
    for (let j = 0; j < concurrent; j++) {
      let id = uuid.v4();
      if (!id) {
        throw new Error('uuid is falsy!?!?');
      }
      uuids.push(id);
      p.push(request({
        id,
        method: 'PUT',
        body: JSON.stringify({
          todoId: id,
          priority: Math.floor(Math.random() * 10) + 1,
          title: 'clean kitchen',
          body: 'make sure the kitchen is clean',
          due: new Date(),
        }),
      }));
    }
    await Promise.all(p);
  }
  testFooter({start, n, msg: 'put requests'});

  start = testHeader({msg: 'get requests'});
  for (let i = 0 ; i < n; i += concurrent) {
    let p = [];
    for (let j = 0; j < concurrent; j++) {
      p.push(request({id: uuids[i+j]}));
    }
    await Promise.all(p);
  }
  testFooter({start, n, msg: 'get requests'});

  start = testHeader({msg: 'post requests'});
  for (let i = 0 ; i < n; i += concurrent) {
    let p = [];
    for (let j = 0; j < concurrent; j++) {
      let id = uuids[i+j];
      p.push(request({
        id,
        method: 'POST',
        body: JSON.stringify({
          todoId: id,
          priority: Math.floor(Math.random() * 10) + 1,
          title: 'clean kitchen',
          body: 'make sure the kitchen is clean',
          due: new Date(),
        }),
      }));
    }
    await Promise.all(p);
  }
  testFooter({start, n, msg: 'post requests'}); 

  start = testHeader({msg: 'delete requests'});
  for (let i = 0 ; i < n; i += concurrent) {
    let p = [];
    for (let j = 0; j < concurrent; j++) {
      p.push(request({id: uuids[i+j], method: 'DELETE'}));
    }
    await Promise.all(p);
  }
  testFooter({start, n, msg: 'delete requests'});

  start = testHeader({msg: 'blended'});

  async function makeTest() {
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
  }

  let tests = n;
  while (tests) {
    let p = [];
    for (let x = 0 ; x < concurrent; x++) {
      p.push(makeTest());
    }
    await Promise.all(p);
    tests -= concurrent;
  }

  testFooter({start, n, msg: 'blended'});
}

main().then(console.log, console.error);

