# go_db: A simple relational database implementation entirely in Golang #

The go_db project contains a simple yet efficient implementation of a relational database.
Currently supported SQL operations:
<ul>
  <li>select</li>
    <ul>
        <li> specific columns </li>
        <li> where clause </li>
        <li> order by </li>
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
conn, err := Connect(dbPath)
if err != nil {
    return err
}
cursor := conn.OpenCursor()
err = cursor.Execute("Select columnA, ColumnB from newTable where columnA = 5 order by columnA")
if err != nil {
    return err
}
records := cursor.FetchAll()
```

 ## Implementation Details ##

 The entire data of the database is stored within a single data blob.
 This data blob can either be saved as a local file, mapped as a buffer in memory, or implemented in any other way that supports the IoInterface interface which is defined in io.go.
 The data blob starts off with a predefined header which points to the locations in the blob where the rest of the data is stored, such as information about the different tables in the DB, and the records within them.

 The DB data blob is divided to "data blocks" which are constant-sized intervals within it, each containing data belonging to a single logical entity, for example: a record's field.
 Each "data block" inside the DB data blob can be either free or taken, as indicated by the free block bitmap, which contains a variable size of bits, each corresponding to a single data block, whose offset is identical to the offset of the bit inside the bitmap.

 For more information refer to the documentation in db.go.

## What's next ##
 Features I've yet to implement:
 * Concurrent access to the DB
 * Indexing
 * Support for join operations
 * Views
 * Robust UI

 Interested in collaborating? Reach out to me :)