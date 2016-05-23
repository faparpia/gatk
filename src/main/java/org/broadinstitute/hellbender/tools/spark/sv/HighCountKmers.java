package org.broadinstitute.hellbender.tools.spark.sv;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.spark.Accumulator;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.programgroups.SparkProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.tools.spark.utils.HopscotchHashSet;
import org.broadinstitute.hellbender.tools.spark.utils.MapPartitioner;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;

/**
 * Tool to describe reads that support a hypothesis of a genomic breakpoint.
 */
@CommandLineProgramProperties(summary="Find reads that evidence breakpoints.",
        oneLineSummary="Dump FASTQs for local assembly of putative genomic breakpoints.",
        programGroup = SparkProgramGroup.class)
public final class HighCountKmers extends GATKSparkTool {
    private static final long serialVersionUID = 1L;
    private static final int MIN_HF_KMER_COUNT = 200;

    @Argument(doc = "run in pure Spark mode", fullName = "pureSpark", optional = true)
    private String pureSpark = "false";

    @Override
    public boolean requiresReads() { return true; }

    @Override
    protected void runTool( final JavaSparkContext ctx ) {
        final List<SVKmer> kmerList;
        final JavaRDD<GATKRead> reads = getUnfilteredReads();
        final int nPartitions = reads.partitions().size();

        if ( pureSpark.equals("true") ) {
            kmerList =
                reads
                    .filter(read ->
                            !read.isSecondaryAlignment() && !read.isSupplementaryAlignment() &&
                                    !read.isDuplicate() && !read.failsVendorQualityCheck())
                    .mapPartitionsToPair(readItr ->
                            new MapPartitioner<GATKRead, Tuple2<SVKmer, Integer>>(readItr, read ->
                                    SVKmerizer.stream(read.getBases(), SVConstants.KMER_SIZE)
                                            .map(kmer -> kmer.canonical(SVConstants.KMER_SIZE))
                                            .map(kmer -> new Tuple2<>(kmer, 1)).iterator()), false)
                    .reduceByKey(Integer::sum)
                    .filter(t2 -> t2._2 >= MIN_HF_KMER_COUNT)
                    .map(t2 -> t2._1)
                    .collect();
        } else {
            kmerList =
                reads
                    .filter(read ->
                            !read.isSecondaryAlignment() && !read.isSupplementaryAlignment() &&
                                    !read.isDuplicate() && !read.failsVendorQualityCheck())
                    .mapPartitions(KmerCounter::new, false)
                    .mapToPair(kmerAndCount -> new Tuple2<>(new SVKmer(kmerAndCount), kmerAndCount.getCount()))
                    .combineByKey(i -> i, Integer::sum, Integer::sum, new HashPartitioner(nPartitions), false, null)
                    .filter(t2 -> t2._2 >= MIN_HF_KMER_COUNT)
                    .map(t2 -> t2._1)
                    .collect();
        }

        System.out.println("There are "+kmerList.size()+" kmers at "+MIN_HF_KMER_COUNT+"x or higher.");
    }

    /**
     * A <Kmer,count> pair.
     * Note:  hashCode and equality does not depend on count, and that's on purpose.
     * This is actually a compacted (K,V) pair, and the hashCode and equality is on K only.
     */
    @DefaultSerializer(KmerAndCount.Serializer.class)
    private final static class KmerAndCount extends SVKmer {
        private int count;

        KmerAndCount(final SVKmer kmer ) {
            super(kmer);
            this.count = 0;
        }

        private KmerAndCount(final Kryo kryo, final Input input ) {
            super(kryo, input);
            count = input.readInt();
        }

        protected void serialize( final Kryo kryo, final Output output ) {
            super.serialize(kryo, output);
            output.writeInt(count);
        }

        @Override
        public boolean equals( final Object obj ) {
            return obj instanceof KmerAndCount && equals((KmerAndCount)obj);
        }

        public boolean equals( final KmerAndCount that ) {
            return super.equals(that);
        }

        public int getCount() { return count; }
        public void incrementCount() { count += 1; }

        @Override
        public String toString() { return super.toString(SVConstants.KMER_SIZE)+" "+count; }

        public static final class Serializer extends com.esotericsoftware.kryo.Serializer<KmerAndCount> {
            @Override
            public void write( final Kryo kryo, final Output output, final KmerAndCount kmerAndCount) {
                kmerAndCount.serialize(kryo, output);
            }

            @Override
            public KmerAndCount read( final Kryo kryo, final Input input, final Class<KmerAndCount> klass ) {
                return new KmerAndCount(kryo, input);
            }
        }
    }

    private final static class KmerCounter implements Iterable<KmerAndCount> {
        private static final int KMERS_PER_PARTITION_GUESS = 20000000;
        private static final int MIN_KMER_COUNT = 2;
        private final HopscotchHashSet<KmerAndCount> kmerSet;

        KmerCounter( final Iterator<GATKRead> readItr ) {
            kmerSet = new HopscotchHashSet<>(KMERS_PER_PARTITION_GUESS);
            while ( readItr.hasNext() ) {
                final Iterator<SVKmer> kmers = new SVKmerizer(readItr.next().getBases(),SVConstants.KMER_SIZE);
                while ( kmers.hasNext() ) {
                    kmerSet.put(new KmerAndCount(kmers.next().canonical(SVConstants.KMER_SIZE))).incrementCount();
                }
            }
//            final Iterator<KmerAndCount> kmerItr = kmerSet.iterator();
//            while ( kmerItr.hasNext() ) {
//                if ( kmerItr.next().getCount() < MIN_KMER_COUNT ) kmerItr.remove();
//            }
        }

        @Override
        public Iterator<KmerAndCount> iterator() { return kmerSet.iterator(); }
    }
}
