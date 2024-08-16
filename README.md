# ObjectKV

A object storage native key-value database with local disk caching.

Modified 2-level LSM architecture.

## Features

- Consistent operations (CAS, update if exists, write if not exists, delete if exists, etc.)
- Write batching for high performance
- Atomic batches with early abort
- Support for read replicas

## Motivation

There have been a lot of object storage native services that push the limits of what we think about in terms of tenancy, price:performance ratios, and maximizing the cloud (e.g. neon.tech and turbopuffer).

I wanted to get in on the action with a key-value database designed for the cost-efficiencies of object storage, the high elasticity of cloud compute, and the speed of modern networking.

Really great KV DBs exist already, such as FoundationDB.

However this has a relatively high TCO, both in terms of management and the (minimum) resources required to run.

Especially with disk.

Traditional databases handle high distributed durability via replication: Keeping multiple compies of the data across different machines. This is great, but a single copy of that data costs 3-4x more than a replicated version on object storage.

But object storage is not a silver bullet.

There are issues like slow and dropped requests, API rate limits, locality, high time-to-first-byte (upwards of 86ms p50 on AWS S3 standard!). But, these pitfalls can be accounted for. It's kind of like designing a database to run on really unreliable spinning disks (which, to be extremely reductionist, is effectively what object storage is).

I wanted to make a KV that can be used at the scale of object storage: Unlimited.

But I didn't want to sacrifice performance, features, or capabilities found in other systems (except maybe super low latency writes). I also wanted it to be a good foundation to build higher-level systems on top, for example transparent large-blob storage by storing references to files in S3, "remote blobs" by storing URLs (e.g. Laion 5B dataset), indexes defined by JS functions instead of SQL, compression, multitenancy, and more.

I also like "serverless" databases in the sense that when you're not actively querying or writing, the only costs are storage. As long as you can match that with lighting-fast cold-boots and disk caching to reduce the latency of subsequent operations, you've got something awesome that works great at both small and massive scale.

I also just find these spaces (storage, cost optimization, multi-tenancy, and distributed systems, big datasets) absolutely fascinating and extremely rewarding to build in.

## Limitations

There's no such thing as a key with an empty value, as that's what we use for tombstones. If you write a key with an empty value, that's the same as deleting.

If you want to make a set, just write a single byte value for the key.