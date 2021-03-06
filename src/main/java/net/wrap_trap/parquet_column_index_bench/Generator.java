package net.wrap_trap.parquet_column_index_bench;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Generator {

  static String TEST_FILE = "src/test/resources/v11.parquet";

  public static void main(String[] args) throws IOException, ParseException {
    generateParquetFile(TEST_FILE);
  }

  private static void generateParquetFile(String path) throws IOException, ParseException {
    File f = new File(path);
    DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    Date date = sdf.parse("2019-02-05 21:41:15.123");

    Configuration conf = new Configuration();
    MessageType schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType(
      "message test { "
        + "required int32 int32_field; "
        + "required int64 int64_field; "
        + "required float float_field; "
        + "required double double_field; "
        + "required binary binary_field; "
        + "required int64 timestamp_field (TIMESTAMP_MILLIS);"
        + "} ");
    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory fact = new SimpleGroupFactory(schema);
    Path fsPath = new Path(f.getPath());
    ParquetWriter<Group> writer = new ParquetWriter<Group>(
      fsPath,
      new GroupWriteSupport(),
      CompressionCodecName.UNCOMPRESSED,
      ParquetWriter.DEFAULT_BLOCK_SIZE,
      ParquetWriter.DEFAULT_PAGE_SIZE,
      256,
      true,
      false,
      ParquetProperties.WriterVersion.PARQUET_2_0,
      conf);
    try {
      for (int i = 0; i < 1024 * 1024 * 10; i++) {
        writer.write(fact.newGroup()
          .append("int32_field", 32 + i)
          .append("int64_field", 64L + i)
          .append("float_field", 1.0f + i)
          .append("double_field", 2.0d + i)
          .append("binary_field", Binary.fromString("abcdefghijklmnopqrstuvwxyz" + i))
          .append("timestamp_field", date.getTime() + (i * 1000)));
      }
    } finally {
      writer.close();
    }
  }
}
