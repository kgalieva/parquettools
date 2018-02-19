package tools

import java.util
import tools.protos.Proto2Schema.Proto2Array
import tools.protos.Proto3Schema.Proto3Array
import org.apache.parquet.proto.ProtoParquetWriter
import org.apache.hadoop.fs.Path

object WriteProtobufParquet {

  private val rnd = new scala.util.Random

  private def createProto2Message = Proto2Array.newBuilder()
    .addAllMyList(util.Arrays.asList(rnd.nextInt(), rnd.nextInt(), rnd.nextInt()))
    .build()

  private def createProto3Message = Proto3Array.newBuilder()
    .addAllMyList(util.Arrays.asList(rnd.nextInt(), rnd.nextInt(), rnd.nextInt()))
    .build()

  private def writeProto2Messages(path: String): Unit = {
    val parquetWriter = new ProtoParquetWriter[Proto2Array](new Path(path), classOf[Proto2Array])
    parquetWriter.write(createProto2Message)
    parquetWriter.write(Proto2Array.getDefaultInstance)
    parquetWriter.write(createProto2Message)
    parquetWriter.close()
  }

  private def writeProto3Messages(path: String): Unit = {
    val parquetWriter = new ProtoParquetWriter[Proto3Array](new Path(path), classOf[Proto3Array])
    parquetWriter.write(createProto3Message)
    parquetWriter.write(Proto3Array.getDefaultInstance)
    parquetWriter.write(createProto3Message)
    parquetWriter.close()
  }

  def main(args: Array[String]) {
    writeProto2Messages("/opt/presto/parquet/proto2/proto2_array.parquet")
    writeProto3Messages("/opt/presto/parquet/proto3/proto3_array.parquet")
  }

}
