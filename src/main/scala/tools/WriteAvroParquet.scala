package tools

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter

object WriteAvroParquet {

  def writeToAvroParquetFile(data: Iterable[GenericRecord], schema: Schema, conf: Configuration, path: String): Unit = {
    val writer = AvroParquetWriter.builder[GenericRecord](new Path(path))
      .withSchema(schema)
      .withConf(conf)
      .build()
    data.foreach(writer.write)
    writer.close()
  }

  def main(args: Array[String]) {

    val NEW_BEHAVIOR: Configuration = new Configuration()
    NEW_BEHAVIOR.setBoolean("parquet.avro.add-list-element-records", false)
    NEW_BEHAVIOR.setBoolean("parquet.avro.write-old-list-structure", false)

    val schema = new Schema.Parser().parse(this.getClass.getResourceAsStream("/array_schema.avsc"))

    val arr1 = new GenericRecordBuilder(schema)
      .set("my_list", Array[Any](1, 5))
      .build()

    val arr2 = new GenericRecordBuilder(schema)
      .set("my_list", Array(5, null, 7, null))
      .build()

    val arr3 = new GenericRecordBuilder(schema)
      .set("my_list", Array.empty)
      .build()

    val arr4 = new GenericRecordBuilder(schema).build()

    val arr5 = new GenericRecordBuilder(schema)
      .set("my_list", Array[Any](9, 12, 15))
      .build()

    var data = List(arr1, arr2, arr3, arr4, arr5)
    writeToAvroParquetFile(data, schema, NEW_BEHAVIOR, "/opt/presto/parquet/avro_new/avro_array_new.parquet")
    data = List(arr1, arr3, arr4, arr5)
    writeToAvroParquetFile(data, schema, new Configuration(), "/opt/presto/parquet/avro_old/avro_array_old.parquet")
  }

}
