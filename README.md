parquet-mr-example
==================

How to use Parquet with Avro as an object model in MapReduce.

So the issue with using the Parquet Group class is when your schema is very large.  The mapper needs to create the String representation of your record each time for every record, and it's taking a very long time to do so.  On top of it, you need to split the resulting string to do processing.

As you know, you can't use the Group class as an intermediate value between Mapper and Reducer, either, because it's not serializable.  Thus, you'll need to use a different object model that is more efficient.

Enter Avro.  Avro and Parquet play very nicely together and is the preferred object model when dealing with Avro.  Just to clarify, we are talking about the in-memory object model here.  Parquet's "default example" is the Group, but you can also use Thrift, Google Protocol Buffers, Hive, or Pig.  But the storage format, on disk, is still Parquet.  You still get the benefits of the columnar data format and compression.

On top of it, you can get all of the benefits of an Avro object model.  You can even get compile-time support for your object model by generating code from the Avro schema.
