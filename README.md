NOTE: this library is under initial development

This is a library for storing objects with a defined structure
and optionally attached behaviour

# Design Goals
This library was written to replace the Azure Entities library which the
Taskcluster platform was built upon.  There were three major motivations for
building this library: redeployability, visibility into data and an
increasingly unreliable Azure service.

## Similar interface to Azure Entities
The Azure Entities library has a very nice interface for being able to build
services which are reliable and scalable by default.  This library is designed
to take the concepts of the Azure Entities library and apply them to a library
backed by Postgres instead.

## Reliable ETag and Last-Modified values
When used in a backend server, there's a very strong chance that each entity
represents an API entity.  For HTTP based services, having reliable etag and
last-modified headers enables more advanced request handling.  Since these
values are both reflections of the data being stored in the database, it would
be ideal if the persistence layer could automatically track when changes are
made to the underlying data.

This library accomplishes this by having two columns for the etag and last
modified.  Both are set as the default value on the table for initial insertion
and all managed tables have a stored procedure that is triggered on updates
which creates new values if and only if the values are changed.

## Zero downtime service upgrades
One of the nice features of Azure Entities is the ability to perform zero
downtime upgrades.  This was accomplished by having a firm storage format in
the data storage system and a user definable format for entries.  A user of the
library can define the structure of the documents they want to store by
describing the fields, marking some as being part of the unique identifier for
that document.  Each version of the document contains a version number and a
function which can do a migration from the previous version.

The rough flow of an upgrade for this library would be:

1. Deploy services with newest version of the entity declaration
1. Once all services have newest version, initiate a migration for all entities
   stored
1. Once migration is completed, the code for older versions can safely be
   removed from the service if desired

During the time between the service deployment beginning and being completed it
is possible that an old service instance tries to read an entity from a newer
version.  In this case, the library will refuse to load the entity by throwing
an error.  Since services should be implemented in a way where requests are
safe to retry, the next attempt by the client to perform that change will
hopefully be picked up by a newer instance of the service.

## Understandable Data Format
Azure Entities is unfortunately very complicated for other consumers to
understand.  Looking at the data without using Azure Entities was basically
impossible.  Without having the actual azure credentials, it is also impossible
to run queries directly on the dataset.  Even with those credentials, no tools
could understand the data stored within.

A goal of this library is to have a data storage format that a human can
understand just looking at it.  This enables easier debugging as well as
enabling others to develop tools which can perform queries on the data stored
in the database.  The data format used by this library is explained later in
this document.

The goal here isn't to enable the database as an interchange format, but rather
allow scripts to be written which examine the queue.  This would ideally be
done using a read-only clone of the database.

## Reliable Unit Tests without Database Configuration
A major goal of this library is to enable users to write unit tests which
require absolutely zero database configuration.  This library contains all the
tooling required to set up a local database that is specifically for running
tests against, managing that process as well as showing logs.  It is also
design to recreate the database from scratch for each test.

Furthermore, the entities are able to generate a complete set of queries which
can create every needed Postgres object (e.g. Tables, Triggers, Indexes,
Extensions, etc) to run the code against.  This means that it is not possible
to forget which database objects need to be created, since the tests won't pass
without them.  It also means that each test runs in a clean database to avoid
intermittent test failures.

The only dependency on the system besides a supported Node.js environment is
that the system have a working `postmaster` and `initdb` command.  Optionally a
`psql` local command can be used to run sql files and commands through psql.
Managing the `postmaster` process and setting up the database directory with
`initdb` (`PGDATA`) is completely managed by the test harness provided.  By
setting the `POSTGRES_LOG_STDIO` variable to any value, the Postgres log file
can be written to stanard output with verbose logging to show the actual
queries being run by Postgres Entities.

In addition to the benefits for tests being more reliable, it also ensures that
the database tests can be run without having any impact on the system
installation of Postgres.  When testing in CI environments, root is not
required for anything other than ensuring Postgres is itself installed.

These tools are exported for use by services built with this library to have
automatic database management in unit tests.

In order to ensure that the system's database is within acceptable version
numebers, these tools support optionally defining a maximum and minimum
supported version of Postgres.  For example, to use the `ON CONFLICT` support
in the `INSERT` command, you must have Postgres 9.5 or higher.  In this case,
the maximum can be specified to this library.  If the tests are attempted on a
Postgres 9.4 system, an error will be raised.

## Optimistic Concurrency
Reducing the amount of time that locks are held improves overall system
performance in many cases.  This library as well as Azure Entities do all edits
of documents using an optimistic concurrency model.  Users will load a
document, perform the changes required and then update the document in the
database if and only if the database's version of the etag matches that of the
document on which the modifications were changed.

Ths simple approach to building this library would have been to edit each
document in a SQL transaction.  The downside to this approach is that a row
lock is held while the entire transaction is run.  If the operation to change
the document takes a very long time, it could cause delays in other request.

In this library, the user will load a copy of the document from the database
and hold absolutely no locks.  In fact, this load can be executed against a
read only copy of the database.  The user can then do whatever is needed to
modify the document.  Only when the document is finished being edited, Postgres
Entities will attempt to modify the data.  In order to safely do the update
only if the etag has not changed, a small transaction is done to read the etag
in and check if it's changed.  This data persistence does hold a row lock for a
short time for inserting into the database.

In the case that there is a collision (i.e. something else modified the data)
an identifiable error is raised and the service can reload the object and try
again.

## No SQL commands in services
Users of this library should not need to write any SQL commands to accomplish
any of the supported operations.

## Database Deployment Options
This library supports running each service in a seperate database as well as
running multiple services in a single database.  For users of this library,
each service sees the same view of the database whether it is the only service
in the database or not.

There's also support for read-only mirror databases for operations which that
is supported.  By default, all operations are done against a read/write
connection.  If so configured, the database can direct operations which only
involve a view of the data to a read only copy.

## TODO Searching
This is not yet implemented, but this library is designed to make it possible
to search for documents based on strict conditions and stream results.

## TODO Queueing
Azure Entities did not have support internally for building queues.  In order
to build queues, the Azure Queue Storage system was instead used.  The drawback
to this queue is that there is zero visibilty into the contents of the queue
while it's in operation and it can only be modified by pushing and popping.
This library will support building work queues of documents inside of it.

This will allow more advanced queueing systems to be build as well as the
ability to query the queue for information about queued items.

# Data Storage Format
The database table format is designed to be static, with the user facing
contents being able to migrate between different definitions as required.

There are a few different levels of storage in this library and it's important
to understand the difference between each one to understand the internal
operation of the library

## Document JS-Values
Documents are JS objects with a few special properties:

1. Each property defined directly on the object is considered to be a 'Field'.
1. Each field has special behaviour for converting to the database
1. The constructor of the Document must take exactly an object of values for
   the fields and set those as own properties
1. Each object must not alter the `.__etag`, `.__fields` or `.__version`
   properties

This level defines the values which services will operate on.

### Ids
Each Entity can define specific properties which make up a part of the ID for
the object in the database.  It is a list of field names, and the system will
determine the key of each document based on values of the document.  These ids
are duplicated from the document into the database column because the `PRIMARY
KEY` constraint of a table does not support being applied to members of a JSON
column.

Where needed to be specified, IDs must always be given in Document JS-Values.
When there is a single field which is the ID field for that entity, a string
can be used as short hand.  Otherwise any object which has properties named the
ID fields with valid values, including a document, can form the id as passesd
around

Take a hypothetical PGEntity instance which has an `id` value of
`['workerGroup', 'workerId']`.  A valid key would be `{workerGroup:
'us-east-1', workerId: 'i-abcdef1234567'}`.  These keys can be over specified
to allow passing an entire document to derive the key from.

In another example where the `id` value is `['taskId']`, a valid key could be
`'abced1234'` as a shorthand.

## Raw JS-Values
Between the Document JS-Values and those values passed through serialised JSON
to Postgres are JS-Values which represent the values which will be JSON
serialised.  Each type of field understands how to convert from database format
using its `fromDB` method and into database format using its `toDB` method.

An example is for the BigInt type.  In Document JS-Values, this is represented
as an actual BigInt variable.  In Raw JS-Values, it is represented as a
hexidecimal string (e.g. `'0xDEADBEEF'`) so that it can be accurately be
serialised to JSON for storage.

When serialising, properties defined on documents which aren't fields will be
ignored.  This is so that any behaviours associated with the document can add
intermediate data as properties on the document.

## Postgres Values
The Postgres table has the following keys:

1. `id TEXT`: the id of the document.  It is duplicated from the document's
   values because of restrictions of the `PRIMARY KEY` constraint in Postgres.
   The value stored here is the serialised format.
1. `value JSONB`: a json document which has the complete contents of the
   document.  This document is a json document where each top level property is
   one of the defined fields.
1. `touched TIMESTAMPTZ`: a timestamp of the last time this document was
   *modified*.  This value is automatically managed and should not be edited.
1. `etag UUID`: a uuid value for this document.  This value is automatically
   managed and should not be edited.  It is a v4 uuid generated by the
   `pgcrypto` library, which internally uses OpenSSL.

There is a dependency on the `pgcrypto` Postgres extension.  This extension is
currently provided by Amazon RDS, Google Cloud SQL, Azure Postgres, Heroku
Postgres, Fedora Postgres and Debian Postgres.

### ID Serialisation Format
The ID used in the database is the base64 encoded string value of the fields
from the document.  The order of fields specified in the PGEntity definition
is the order of the fields in the id.

This code will generate a valid database id column value for a PGEntity which
uses an id value of `['name', 'vorname']`:

```javascript
`${Buffer.from('Ford').toString('base64')}_${Buffer.from('John').toString('base64')}`
```

This format is used so that the simplist of a single field key has a simple
mapping to database id, while still supporting composite keys.

# Migration
Each migration is performed by a function which receives a copy of the field
values of a document.  It does not receive an actual document itself.  This is
to ensure that when a document has behaviour attached to it (e.g. methods)
those methods only need to be defined for the latest version.  Migration
functions should be removed once there's no longer a chance of them being found
in the wild.

Migration occurs on all loads of documents which are at a version lower than
the current version.  It only occurs on loads, and loading does not persist the
migration.  This is to support read-only databases.  Since the migration is
done on every load, all update operations will result in the document being
stored with the new version.

# Examples
As is often the case, the unit tests for this library serve as the best
examples for usage.  Some specific examples are provided here to illustrate the
library.

## Imports
This the maximum extent of imported constructors to use this library
```javascript
const {
  PGEntityError,
  PGEntityManager,
  PGEntity,
  PGEntityDocument,
  PGStringField,
  PGIntegerField,
  PGBigIntField,
  PGDateField,
} = require('postgres-entities');
```

If you're building a service using `taskcluster-lib-loader`, the
`PGEntityManager` should be instantiated once for the whole service, one Entity
for type to be persisted and any custom `DocumentConstructor` classes should be
built as code outside of the loader file.

## Define an Entity
```javascript
let manager = new PGEntityManager({
  service: 'phonebook',
  connectionString: process.env.DATABASE_URL,
});

let taskEntity = new PGEntity({
  name: 'task',
  id: ['taskId'],
  manager,
  versions: [{
    fields: {
      taskId: PGStringField,
      command: PGStringField,
      priority: PGIntegerField,
    },
    indexes: ['priority'],
  }],
});
```

## Define an Entity with a custom Document type
If so desired, a different constructor can be given to the `PGEntity`
constructor.

This is useful when custom behaviour is desired.

```javascript

class Task extends PGEntityDocument {
  priorityFactor(x) {
    return this.priority * x;
  }
}

let taskEntity = new PGEntity({
  name: 'task',
  id: ['taskId'],
  manager,
  DocumentConstructor: Task,
  versions: [{
    fields: {
      taskId: PGStringField,
      command: PGStringField,
      priority: PGIntegerField,
    },
    indexes: ['priority'],
  }],
});
```

## Create, Insert, View, Update and Remove an Entity
```javascript
let document = taskEntity.createDocument({
  value: {
    taskId: slugid.nice(),
    command: 'echo hello, world',
    priority: 10,
});

await taskEntity.insert(document);

await taskEntity.update(document, () => {
  document.priority = 20;
});

await taskEntity.remove(document);
```

## Acknowledgments
[Jonas](https://github.com/jonasfj) built the library that served as
inspriation for a lot of the design.  This library has served the Taskcluster
team very well for nearly 5 years and has been key to the ability to rapidly
build and iterate on services.
