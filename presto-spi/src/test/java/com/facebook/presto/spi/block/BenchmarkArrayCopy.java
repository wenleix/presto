package com.facebook.presto.spi.block;

import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.IntegerType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5)
@Fork(3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkArrayCopy
{
    private static final int POSITIONS = 100_000;

    /*
    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public BlockBuilder benchmarkWritePosition(BenchmarkData data)
    {
        Block block = data.getDataBlock();
        BlockBuilder blockBuilder = data.getBlockBuilder();

        for (int i = 0; i < POSITIONS; i++) {
            if (block.isNull(i)) {
                blockBuilder.appendNull();
            }
            else {
                block.writePositionTo(i, blockBuilder);
                blockBuilder.closeEntry();
            }
        }

        return blockBuilder;
    }
    */

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public BlockBuilder benchmarkAppendPosition(BenchmarkData data)
    {
        Block block = data.getDataBlock();
        BlockBuilder blockBuilder = data.getBlockBuilder();

        for (int i = 0; i < POSITIONS; i++) {
            block.appendPositionTo(i, blockBuilder);
        }

        return blockBuilder;
    }

    /*
    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public BlockBuilder benchmarkAppendRegion(BenchmarkData data)
    {
        Block block = data.getDataBlock();
        BlockBuilder blockBuilder = data.getBlockBuilder();

        block.appendRegionTo(0, POSITIONS, blockBuilder);
        return blockBuilder;
    }
    */

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"1", "2", "4", "8", "16"})
        private int arrayLength;

        private Block dataBlock;
        private BlockBuilder blockBuilder;
        private BlockBuilderStatus status;

        @Setup
        public void setup()
        {
            ArrayType arrayType = new ArrayType(IntegerType.INTEGER);
            status = new BlockBuilderStatus();
            blockBuilder = arrayType.createBlockBuilder(status, POSITIONS);
            for (int position = 0; position < POSITIONS; position++) {
                BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
                for (int i = 0; i < arrayLength; i++) {
                    arrayType.getElementType().writeLong(entryBuilder, ThreadLocalRandom.current().nextInt());
                }
                blockBuilder.closeEntry();
            }

            dataBlock = blockBuilder.build();
        }

        public Block getDataBlock()
        {
            return dataBlock;
        }

        public BlockBuilder getBlockBuilder()
        {
            return blockBuilder.newBlockBuilderLike(status);
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
//        new BenchmarkArrayCopy().benchmarkWritePosition(data);
        new BenchmarkArrayCopy().benchmarkAppendPosition(data);
//        new BenchmarkArrayCopy().benchmarkAppendRegion(data);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkArrayCopy.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }

}
