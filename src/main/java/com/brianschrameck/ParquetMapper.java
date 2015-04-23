package com.brianschrameck;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ParquetMapper extends Mapper<LongWritable, GenericRecord, Text, AvroValue<GenericRecord>> {
    /**
     * The ASCII value of CTRL+A.
     */
    private static final char WRITE_DELIM = 1;

    // Reuse output objects.
    private final Text outputKey = new Text();
    private final AvroValue<GenericRecord> outputValue = new AvroValue<GenericRecord>();

    /**
     * Just creates a simple key using two fields and passes the rest of the value directly through.
     * Do some more processing here.
     *
     * @param key     the mapper's key -- not used
     * @param value   the Avro representation of this Parquet record
     * @param context the mapper's context
     */
    @Override
    protected void map(LongWritable key, GenericRecord value, Context context) throws IOException, InterruptedException {
        outputKey.set("" + value.get("field1") + WRITE_DELIM + value.get("field2"));
        outputValue.datum(value);
        context.write(outputKey, outputValue);
    }
}
