# ObjectKV

A object storage native key-value database with local disk caching.

Modified 2-level LSM architecture.

## Features

- Consistent operations (CAS, update if exists, write if not exists, delete if exists, etc.)
- Write batching for high performance
- Atomic batches with early abort
- Support for read replicas
