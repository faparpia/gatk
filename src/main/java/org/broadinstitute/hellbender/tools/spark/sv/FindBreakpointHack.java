package org.broadinstitute.hellbender.tools.spark.sv;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.programgroups.SparkProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.tools.spark.utils.HopscotchHashSet;
import org.broadinstitute.hellbender.tools.spark.utils.MapPartitioner;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.util.*;
import java.util.function.Function;

/**
 * Tool to describe reads that support a hypothesis of a genomic breakpoint.
 */
@CommandLineProgramProperties(summary="Find reads that evidence breakpoints.",
        oneLineSummary="Dump FASTQs for local assembly of putative genomic breakpoints.",
        programGroup = SparkProgramGroup.class)
public final class FindBreakpointHack extends GATKSparkTool {
    private static final long serialVersionUID = 1L;

    @Override
    public boolean requiresReads()
    {
        return true;
    }

    @Override
    protected void runTool( final JavaSparkContext ctx ) {
        final HopscotchHashSet<KmerAndInterval> kmerAndIntervalsSet = new HopscotchHashSet<>(10481);
        for ( final SVKmer kmer : SVKmer.readKmersFile("45138.kmers", null) ) {
            kmerAndIntervalsSet.add(new KmerAndInterval(kmer,45138));
        }
        final List<String> qNames = getAssemblyQNames(ctx, kmerAndIntervalsSet, getUnfilteredReads());
        for ( final String qName : qNames )
            System.out.println(qName);
    }

    private static List<String> getAssemblyQNames(
            final JavaSparkContext ctx,
            final HopscotchHashSet<KmerAndInterval> kmerAndIntervalsSet,
            final JavaRDD<GATKRead> reads ) {

        final Broadcast<HopscotchHashSet<KmerAndInterval>> broadcastKmerAndIntervalsSet =
                ctx.broadcast(kmerAndIntervalsSet);

        final List<String> qNames =
                reads
                        .filter(read ->
                                !read.isSecondaryAlignment() && !read.isSupplementaryAlignment() &&
                                        !read.isDuplicate() && !read.failsVendorQualityCheck())
                        .mapPartitions(readItr ->
                                new MapPartitioner<>(readItr,
                                        new QNamesForKmersFinder(broadcastKmerAndIntervalsSet.value())), false)
                        .collect();

        broadcastKmerAndIntervalsSet.destroy();

        return qNames;
    }


    /**
     * A <Kmer,IntervalId> pair.
     * Note:  hashCode does not depend on intervalId, and that's on purpose.
     * This is actually a compacted (K,V) pair, and the hashCode is on K only.
     */
    @DefaultSerializer(KmerAndInterval.Serializer.class)
    private final static class KmerAndInterval extends SVKmer {
        private final int intervalId;

        KmerAndInterval(final SVKmer kmer, final int intervalId ) {
            super(kmer);
            this.intervalId = intervalId;
        }

        private KmerAndInterval(final Kryo kryo, final Input input ) {
            super(kryo, input);
            intervalId = input.readInt();
        }

        protected void serialize( final Kryo kryo, final Output output ) {
            super.serialize(kryo, output);
            output.writeInt(intervalId);
        }

        @Override
        public boolean equals( final Object obj ) {
            return obj instanceof KmerAndInterval && equals((KmerAndInterval)obj);
        }

        public boolean equals( final KmerAndInterval that ) {
            return super.equals(that) && this.intervalId == that.intervalId;
        }

        @Override
        public String toString() { return super.toString(SVConstants.KMER_SIZE)+" "+intervalId; }

        public static final class Serializer extends com.esotericsoftware.kryo.Serializer<KmerAndInterval> {
            @Override
            public void write( final Kryo kryo, final Output output, final KmerAndInterval kmerAndInterval) {
                kmerAndInterval.serialize(kryo, output);
            }

            @Override
            public KmerAndInterval read(final Kryo kryo, final Input input,
                                        final Class<KmerAndInterval> klass ) {
                return new KmerAndInterval(kryo, input);
            }
        }
    }


    /**
     * Class that acts as a mapper from a stream of reads to a stream of <intervalId,read> pairs.
     * It knows which breakpoint(s) a read belongs to (if any) by kmerizing the read, and looking up each SVKmer in
     * a multi-map of SVKmers onto intervalId.
     */
    private static final class QNamesForKmersFinder implements Function<GATKRead, Iterator<String>> {
        private final HopscotchHashSet<KmerAndInterval> kmerAndIntervalSet;

        QNamesForKmersFinder(final HopscotchHashSet<KmerAndInterval> kmerAndIntervalSet) {
            this.kmerAndIntervalSet = kmerAndIntervalSet;
        }

        public Iterator<String> apply(final GATKRead read) {
            final List<String> strings = new ArrayList<>();
            SVKmerizer.stream(read.getBases(), SVConstants.KMER_SIZE)
                    .map( kmer -> kmer.canonical(SVConstants.KMER_SIZE) )
                    .forEach( kmer -> {
                        final Iterator<KmerAndInterval> itr = kmerAndIntervalSet.bucketIterator(kmer.hashCode());
                        while ( itr.hasNext() ) {
                            final KmerAndInterval kmerAndInterval = itr.next();
                            if (kmer.equals(kmerAndInterval)) {
                                strings.add(read.getName()+" "+kmer.toString(SVConstants.KMER_SIZE));
                            }
                        }
                    });
            return strings.iterator();
        }
    }

}
