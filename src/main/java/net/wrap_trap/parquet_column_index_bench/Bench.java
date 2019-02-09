package net.wrap_trap.parquet_column_index_bench;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.api.Binary;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;

public class Bench {

  @Benchmark
  public void filteringScanWithColumnIndex() throws IOException {
    filteringScan(true);
  }

  @Benchmark
  public void filteringScanWithoutColumnIndex() throws IOException {
    filteringScan(false);
  }

  private void filteringScan(boolean useColumnIndexFilter) throws IOException {
    Path inPath = new Path(Generator.TEST_FILE);
    FilterPredicate actualFilter = eq(binaryColumn("binary_field"), Binary.fromString("abcdefghijklmnopqrstuvwxyz524288"));
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), inPath)
      .withFilter(FilterCompat.get(actualFilter))
      .useColumnIndexFilter(useColumnIndexFilter).build();
    Group group;
    while ((group = reader.read()) != null) {
      assert(group.getBinary("binary_field", 0).toString().equals("abcdefghijklmnopqrstuvwxyz524288"));
    }
  }

  public static void main(String[] args) throws IOException, RunnerException {
    Options opt = new OptionsBuilder()
                    .include(Bench.class.getName())
                    .warmupIterations(5)
                    .forks(1)
                    .mode(Mode.AverageTime)
                    .timeUnit(TimeUnit.MILLISECONDS)
                    .build();
    new Runner(opt).run();
  }
}
