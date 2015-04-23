package com.brianschrameck;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroParquetOutputFormat;
import parquet.avro.AvroSchemaConverter;
import parquet.hadoop.Footer;
import parquet.hadoop.ParquetFileReader;
import parquet.schema.MessageType;

import java.util.List;

/**
 * Example usage: yarn jar parquet-mr-example-0.0.1-SNAPSHOT.jar \
 * -libjars /opt/cloudera/parcels/CDH/lib/avro/avro-mapred.jar -Dmapreduce.job.reduces=11 \
 * -Ddfs.blocksize=256m -Dparquet.block.size=268435456 /user/bschrameck/authParquet/ /user/bschrameck/parquetOutput/
 */
public class ParquetJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ParquetJob(), args);
        System.exit(res);
    }

    /**
     * @param args the arguments, which consist of the input file or directory and the output directory
     * @return the job return status, 0 for success, 1 for failure
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(ParquetJob.class);

        // Parquet Schema
        // we assume a single schema for all files
        List<Footer> footers = ParquetFileReader.readFooters(getConf(), new Path(args[0]));
        MessageType schema = footers.get(0).getParquetMetadata().getFileMetaData().getSchema();

        // Avro Schema
        // convert the Parquet schema to an Avro schema
        AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter();
        Schema avroSchema = avroSchemaConverter.convert(schema);

        // Mapper
        job.setMapperClass(ParquetMapper.class);
        // Input
        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job, new Path(args[0]));
        AvroParquetInputFormat.setAvroReadSchema(job, avroSchema);

        // Intermediate Output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AvroValue.class);
        AvroJob.setMapOutputValueSchema(job, avroSchema);

        // Reducer
        job.setReducerClass(ParquetReducer.class);
        // Output
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setOutputPath(job, new Path(args[1]));
        AvroParquetOutputFormat.setSchema(job, avroSchema);
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(GenericRecord.class);

        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
