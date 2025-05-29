# FileDB

A lightweight, file-based database system written in Clojure that provides a simple yet powerful way to store and manage data in files. FileDB offers collection management, nested collections, automatic ID generation, timestamps, and rich querying capabilities.

## Why?

FileDB was created to solve a common challenge in software development: persisting data during rapid prototyping and early development phases.

Most applications need to store and retrieve data, but traditional databases often require rigid schema definitions upfront. During prototyping, when your data model is constantly evolving as you explore and refine your application's core functionality, this rigidity becomes a hindrance. You end up spending valuable development time adjusting database schemas rather than focusing on validating your core ideas.

FileDB takes a different approach by providing a schema-less, file-based storage solution that allows you to:

- Start persisting data immediately without upfront schema design
- Evolve your data model freely as your prototype develops
- Focus on core functionality instead of database maintenance
- Maintain a simple, transparent data store that's easy to inspect and modify

While FileDB isn't designed to replace production databases, it excels at its intended purpose: enabling rapid iteration during prototyping and early development while solving the fundamental need for data persistence.


## Performance

Is FileDB as fast as PostgreSQL? Hell no! But remember, you have no users yet! 

When you're building something new, your priorities are:

1. Getting core functionality working
2. Iterating quickly on the user experience 
3. Validating that you're solving the right problem

Raw database performance becomes important once you have real users and proven product-market fit. But optimizing too early for performance can slow down the more critical early-stage work of finding the right product.

FileDB intentionally trades performance for:

- Simplicity: No setup required, just start using it
- Flexibility: Change your data model freely without migrations
- Transparency: Data stored in plain text files you can inspect
- Development speed: Focus on product features, not database administration

Once your prototype proves successful and you have real users, that's the right time to invest in migrating to a production-ready database. But until then, FileDB helps you move fast and focus on what matters most - building something people want.

## Features

- **File-based Storage**: Each record is stored as an EDN file
- **Collections & Nested Collections**: Support for both flat and hierarchical data organization
- **Automatic ID Generation**: Built-in sequential ID generation for collections
- **Timestamps**: Automatic created_at and updated_at timestamps
- **Rich Query Interface**: Filter, sort, limit, and paginate results
- **Namespace Support**: Optional namespace qualification for keywords
- **CRUD Operations**: Full support for Create, Read, Update, and Delete operations

## Installation

Add the following dependency to your `deps.edn`:

```clojure
io.github.efraimmgon/filedb {:git/tag "v0.0.1" :git/sha "4c4a34d"}
```

## Quick Start

```clojure
(require '[filedb.core :as db])

;; Use the default database instance
(def db db/default-db)

;; Insert a record
(db/insert! db :users {:name "John Doe" :email "john@example.com"})

;; Query records
(db/get-by-key db :users :name "John Doe")

;; Update a record
(db/update! db :users 1 {:status :active})

;; Delete a record
(db/delete! db :users 1)
```

## Collections

### Basic Collections

```clojure
;; Insert into a basic collection
(db/insert! db :users {:name "Jane Doe"})
```

### Nested Collections

```clojure
;; Insert into a nested collection
(db/insert! db [:users 1 :posts] {:title "Hello World"})
```

## Querying

### Get by ID

```clojure
(db/get-by-id db :users 1)
```

### Get by Key

```clojure
(db/get-by-key db :users :email "john@example.com")
```

### Advanced Queries

```clojure
(db/query db :users 
  {:where #(> (:age %) 21)
   :order-by [:created-at :desc]
   :limit 10
   :offset 0})
```

## Keyword Strategies

FileDB supports different keyword qualification strategies:

- `:simple` - No qualification
- `:partial` - Qualifies with collection name
- `:full` - Full qualification including nested paths

```clojure
;; Create a DB with custom keyword strategy
(def custom-db 
  (->FileDB "custom-db" 
            (assoc db/default-keywords :coll-ns-type :full))) ; :simple, :partial, :full
```

## API Reference

### Core Operations

- `insert!`: Insert a new record
- `get-by-id`: Retrieve a record by ID
- `get-by-ids`: Retrieve multiple records by IDs
- `get-all`: Get all records in a collection
- `update!`: Update an existing record
- `delete!`: Delete a record
- `query`: Advanced querying with filtering and sorting

### Collection Management

- `delete-coll!`: Delete an entire collection
- `reset-db!`: Reset the entire database (deletes the database directory)
- `get-count`: Get the count of records in a collection

## Configuration

The default database root is "filedb", but you can specify a custom location:

```clojure
(def custom-db (->FileDB "custom-location" default-keywords))
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details. 