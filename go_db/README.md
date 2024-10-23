# go_db: A simple relational database implementation entirely in Golang #

The go_db project contains a simple yet efficient implementation of a relational database.
Currently supported SQL operations:
<ul>
  <li>select</li>
    <ul>
        <li> specific columns </li>
        <li> where clause </li>
        <li> order by </li>
        <li> JOINs </li>
        <li> best (explanation ahead) </li>
    </ul>
  <li>insert</li>
  <li>update</li>
  <li>delete</li>
  <li>update</li>
</ul>

 The "where" clause can be used in all operations other than insert (as expected).

## Usage ##
Using the framework is relatively simple - all you've got to do is open a connection to the database, create a cursor object, and use it to execute queries and fetch results:

 ```go
conn, err := Connect(dbPath, nil)
if err != nil {
    return err
}
cursor := conn.OpenCursor()
err = cursor.Execute("Select table1.columnA, table2.ColumnB from table1 join table2 where table1.columnA = 5 order by table2.columnB")
if err != nil {
    return err
}
records := cursor.FetchAll()
```

 ## Implementation Details ##

 ### how data is stored ###

 The entire data of the database is stored within a single data blob.
 This data blob can either be saved as a local file, mapped as a buffer in memory, or implemented in any other way that supports the IoInterface interface which is defined in io.go.
 The data blob starts off with a predefined header which points to the locations in the blob where the rest of the data is stored, such as information about the different tables in the DB, and the records within them.

 The DB data blob is divided to "data blocks" which are constant-sized intervals within it, each containing data belonging to a single logical entity, for example: a record's field.
 Each "data block" inside the DB data blob can be either free or taken, as indicated by the free block bitmap, which contains a variable size of bits, each corresponding to a single data block, whose offset is identical to the offset of the bit inside the bitmap.

 For more information refer to the documentation in db.go.

 ### indexing ###
Indexing is done using a B+ Tree implemented [here](https://github.com/ronGeva/go_apps/tree/main/b_tree). 

Between the B+ Tree and the database lies the [persistency interface](https://github.com/ronGeva/go_apps/blob/main/b_tree/persistency.go#L10).
The database supplies an object that implements this interface and is used by tree to save/load data from a persistent storage.

A noteworthy addition to the tree implementation is the support for non-unique keys.
This is done in [index.go](index.go) by adding another B+ Tree for every group of records with the same key in a given index.
The original index then points to this new tree (instead of pointing to a record), and the same-key tree can be used
to actually retrieve records (in an arbitray order, but that doesn't matter since they all have the same key anyway).

### provenance support ###
The database supports provenance, or in other words metadata about the origin of the data.
By origin we refer to the entity which added data.

Provenance is supplied to the database via the [getOpenDB](db.go#L178) function in the form of the [DBProvenance](provenance.go#L417)
 struct.
DBProvenance contains information about the connection, currently we support:
<ul>
  <li>Authentication Info (username/password)</li>
  <li>Connection Info (network endpoint info)</li>
</ul>

This information is then used to rank the origin of data and give it a reliability score - see 
[ProvenanceScore](provenance.go#L13).

A record's provenance can be accessed via its Provenance field, like this:
 ```go
var record Record

provenanceFields := record.Provenance
provField := provenanceFields[0]
score := provField.Score()
provenanceType := provField.Type
```

### record retrieval according to reliability ###

The database supports retrieving the top most reliable records (according to their provenance of course).

This is done via the normal SELECT query, with the addition of the new keyword "best", which is added after the "order by" clause.
An example of usage is:
 ```go
conn, err := Connect(dbPath, nil)
if err != nil {
    return err
}
cursor := conn.OpenCursor()
err = cursor.Execute("Select costumers.id, receipts.amount from costumers join receipts where costumers.id = receipts.costumerId best 10")
if err != nil {
    return err
}
records := cursor.FetchAll()
```

In this example, we get the 10 most reliable (costumerId, receiptAmount) pairs we have in the database.

The implementation of this logic is mostly in [provenanceGetTopRecords](provenance_record_iterator.go#L467).

## What's next ##
 Features I've yet to implement:
 * Concurrent access to the DB
 * Views
 * Robust UI

 Interested in collaborating? Reach out to me :)