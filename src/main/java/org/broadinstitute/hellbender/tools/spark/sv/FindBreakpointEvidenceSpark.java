package org.broadinstitute.hellbender.tools.spark.sv;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import htsjdk.samtools.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.SparkProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.spark.utils.HopscotchHashSet;
import org.broadinstitute.hellbender.tools.spark.utils.MapPartitioner;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadUtils;
import scala.Tuple2;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;

/**
 * Tool to describe reads that support a hypothesis of a genomic breakpoint.
 */
@CommandLineProgramProperties(summary="Find reads that evidence breakpoints.",
        oneLineSummary="Dump FASTQs for local assembly of putative genomic breakpoints.",
        programGroup = SparkProgramGroup.class)
public final class FindBreakpointEvidenceSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;
    private static final int MIN_MAPQ = 20;
    private static final int MIN_MATCH_LEN = 45; // minimum length of matched portion of an interesting alignment
    //private static final int MAX_FRAGMENT_LEN = 2000;
    private static final int MAX_COVERAGE = 1000;
    private static final float ASSEMBLY_TO_MAPPED_SIZE_RATIO = 7.f;

    private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss ");

    @Argument(doc = "directory for fastq output", shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME)
    private String outputDir;

    @Argument(doc = "directory for evidence output", fullName = "breakpointEvidenceDir", optional = true)
    private String evidenceDir;

    @Argument(doc = "file for breakpoint intervals output", fullName = "breakpointIntervals", optional = true)
    private String intervalFile;

    @Argument(doc = "file for mapped qname intervals output", fullName = "qnameIntervalsMapped", optional = true)
    private String qNamesMappedFile;

    @Argument(doc = "file for kmer intervals output", fullName = "kmerIntervals", optional = true)
    private String kmerFile;

    @Argument(doc = "file for mapped qname intervals output", fullName = "qnameIntervalsForAssembly", optional = true)
    private String qNamesAssemblyFile;

    /**
     * This is a path to a file of kmers that appear too frequently in the reference to be usable as probes to localize
     * reads.  We don't calculate it here, because it depends only on the reference.
     * The program FindBadGenomicKmersSpark can produce such a list for you.
     */
    @Argument(doc = "file containing ubiquitous kmer list", fullName = "kmersToIgnore")
    private String kmersToIgnoreFilename;

    @Override
    public boolean requiresReads()
    {
        return true;
    }

    @Override
    protected void runTool( final JavaSparkContext ctx ) {
        if ( getHeaderForReads().getSortOrder() != SAMFileHeader.SortOrder.coordinate ) {
            throw new GATKException("The reads must be coordinate sorted.");
        }

        final Locations locations =
                new Locations(outputDir, evidenceDir, intervalFile, qNamesMappedFile, kmerFile, qNamesAssemblyFile, kmersToIgnoreFilename);
        final PipelineOptions pipelineOptions = getAuthenticatedGCSOptions();
        final SAMFileHeader header = getHeaderForReads();
        final JavaRDD<GATKRead> reads = getUnfilteredReads();
        final HopscotchHashSet<QNameAndInterval> qNamesSet;
        int nIntervals;
        if ( true ) {
            final HopscotchHashSet<KmerAndInterval> kmerAndIntervalsSet;
            if ( true ) {
                final ReadMetadata readMetadata = getMetadata(header, getMeanBasesPerTemplate(reads));
                final Broadcast<ReadMetadata> broadcastMetadata = ctx.broadcast(readMetadata);
                final List<Interval> intervals =
                        getIntervals(broadcastMetadata, header, reads, locations, pipelineOptions);

                nIntervals = intervals.size();
                if ( nIntervals == 0 ) return;

                qNamesSet = getQNames(ctx, broadcastMetadata, intervals, reads, locations, pipelineOptions);

                broadcastMetadata.destroy();
            }
            kmerAndIntervalsSet = new HopscotchHashSet<>(
                    getKmerIntervals(ctx, qNamesSet, reads, locations, pipelineOptions));

            qNamesSet.addAll(getAssemblyQNames(ctx, kmerAndIntervalsSet, reads));

            if ( locations.qNamesAssemblyFile != null ) {
                dumpQNames(locations.qNamesAssemblyFile, pipelineOptions, qNamesSet);
            }

            log("Discovered "+qNamesSet.size()+" unique template names for assembly.");
        }
        generateFastqs(ctx, qNamesSet, nIntervals, reads, locations);
    }

    private static void generateFastqs( final JavaSparkContext ctx,
                                        final HopscotchHashSet<QNameAndInterval> qNamesSet,
                                        final int nIntervals,
                                        final JavaRDD<GATKRead> reads,
                                        final Locations locations) {
        final Broadcast<HopscotchHashSet<QNameAndInterval>> broadcastQNamesSet = ctx.broadcast(qNamesSet);
        final String outputDir = locations.outputDir;

        reads
            .mapPartitionsToPair(readItr ->
                    new ReadsForQNamesFinder(broadcastQNamesSet.value(), nIntervals).call(readItr), false)
            .reduceByKey(FindBreakpointEvidenceSpark::combineLists)
            .foreach(intervalAndFastqs -> writeFastq(intervalAndFastqs, outputDir));

        broadcastQNamesSet.destroy();
        log("Wrote FASTQs for assembly.");
    }

    private static List<byte[]> combineLists( final List<byte[]> list1, final List<byte[]> list2 ) {
        final List<byte[]> result = new ArrayList<>(list1.size() + list2.size());
        result.addAll(list1);
        result.addAll(list2);
        return result;
    }

    /** write a FASTQ file for an assembly */
    private static void writeFastq( final Tuple2<Integer, List<byte[]>> intervalAndFastqs,
                                    final String outputDir ) {
        final int nFastqs = SVUtils.iteratorSize(intervalAndFastqs._2.iterator());
        final byte[][] fastqsArray = new byte[nFastqs][];
        final Iterator<byte[]> itr = intervalAndFastqs._2.iterator();
        for ( int idx = 0; idx != nFastqs; ++idx ) {
            if ( !itr.hasNext() ) throw new GATKException("fastq iterator crapped out at element "+idx);
            fastqsArray[idx] = itr.next();
        }
        if ( itr.hasNext() ) throw new GATKException("unexpected extra element in fastq iterator");

        // since the fastq bytes start with the read name, this will interleave pairs
        Arrays.sort(fastqsArray, FindBreakpointEvidenceSpark::compareByteArrays);

        // we're not going to try to marshal PipelineOptions for now -- not sure it's a good idea, anyway
        final PipelineOptions pipelineOptions = null;
        final String fileName = outputDir + "/assembly" + intervalAndFastqs._1 + ".fastq";
        try ( final OutputStream writer = new BufferedOutputStream(BucketUtils.createFile(fileName, pipelineOptions)) ) {
            for ( final byte[] fastqBytes : fastqsArray ) {
                writer.write(fastqBytes);
            }
        } catch ( final IOException ioe ) {
            throw new GATKException("Can't write "+fileName, ioe);
        }
    }

    private static int compareByteArrays( final byte[] arr1, final byte[] arr2 ) {
        final int len = Math.min(arr1.length, arr2.length);
        for ( int idx = 0; idx != len; ++idx ) {
            final int result = Integer.compare(arr1[idx] & 0xff, arr2[idx] & 0xff);
            if ( result != 0 ) return result;
        }
        return Integer.compare(arr1.length, arr2.length);
    }

    /**
     * Process the input reads, pulling all reads that contain kmers associated with a given breakpoint,
     * and writing those reads into a separate FASTQ for each breakpoint.
     */
    private static List<QNameAndInterval> getAssemblyQNames(
            final JavaSparkContext ctx,
            final HopscotchHashSet<KmerAndInterval> kmerAndIntervalsSet,
            final JavaRDD<GATKRead> reads ) {
        final Broadcast<HopscotchHashSet<KmerAndInterval>> broadcastKmerAndIntervalsSet =
                ctx.broadcast(kmerAndIntervalsSet);

        final List<QNameAndInterval> qNames =
            reads
                .filter(read ->
                        !read.isSecondaryAlignment() && !read.isSupplementaryAlignment() &&
                                !read.isDuplicate() && !read.failsVendorQualityCheck())
                .mapPartitions(readItr ->
                        new MapPartitioner<>(readItr,
                                new QNamesForKmersFinder(broadcastKmerAndIntervalsSet.value())), false)
                .collect();

        broadcastKmerAndIntervalsSet.destroy();

        log("Discovered " + qNames.size() + " template names among kmers.");
        return qNames;
    }

    /** find kmers for each interval */
    private static List<KmerAndInterval> getKmerIntervals(final JavaSparkContext ctx,
                                                          final HopscotchHashSet<QNameAndInterval> qNamesSet,
                                                          final JavaRDD<GATKRead> reads,
                                                          final Locations locations,
                                                          final PipelineOptions pipelineOptions ) {
        final Broadcast<Set<SVKmer>> broadcastKmerKillSet =
                ctx.broadcast(SVKmer.readKmersFile(locations.kmersToIgnoreFilename, pipelineOptions));
        final Broadcast<HopscotchHashSet<QNameAndInterval>> broadcastQNameAndIntervalsSet =
                ctx.broadcast(qNamesSet);

        // given a set of template names with interval IDs and a kill set of genomically ubiquitous kmers,
        // produce a set of interesting kmers for each interval ID
        final List<KmerAndInterval> kmerIntervals =
            reads
            .filter(read -> !read.isDuplicate() && !read.failsVendorQualityCheck() &&
                    !read.isSecondaryAlignment() && !read.isSupplementaryAlignment())
            .mapPartitionsToPair(readItr ->
                    new MapPartitioner<>(readItr,
                        new QNameKmerizer(broadcastQNameAndIntervalsSet.value(),
                                        broadcastKmerKillSet.value())), false)
            .reduceByKey( (c1, c2) -> c1+c2 )
            .mapPartitions(itr -> new KmerCleaner().call(itr))
            .collect();

        broadcastQNameAndIntervalsSet.destroy();
        broadcastKmerKillSet.destroy();

        // record the kmers with their interval IDs
        if ( locations.kmerFile != null ) {
            try (final OutputStreamWriter writer = new OutputStreamWriter(new BufferedOutputStream(
                    BucketUtils.createFile(locations.kmerFile, pipelineOptions)))) {
                for (final KmerAndInterval kmerAndInterval : kmerIntervals) {
                    writer.write(kmerAndInterval.toString() + "\n");
                }
            } catch (final IOException ioe) {
                throw new GATKException("Can't write kmer intervals file " + locations.kmerFile, ioe);
            }
        }

        log("Discovered "+ kmerIntervals.size() +" kmers.");
        return kmerIntervals;
    }

    /** find template names for reads mapping to each interval */
    private static HopscotchHashSet<QNameAndInterval> getQNames( final JavaSparkContext ctx,
                                                                 final Broadcast<ReadMetadata> broadcastMetadata,
                                                                 final List<Interval> intervals,
                                                                 final JavaRDD<GATKRead> reads,
                                                                 final Locations locations,
                                                                 final PipelineOptions pipelineOptions ) {
        final int meanBasesPerTemplate = broadcastMetadata.value().getMeanBasesPerTemplate();
        final int maxIntervalLength = intervals.stream().mapToInt(Interval::getLength).max().orElse(0);
        final int maxQnamesPerInterval = MAX_COVERAGE*maxIntervalLength/meanBasesPerTemplate;

        final Broadcast<List<Interval>> broadcastIntervals = ctx.broadcast(intervals);
        final List<QNameAndInterval> qNameAndIntervalList =
                reads
                    .filter(read -> !read.isDuplicate() && !read.failsVendorQualityCheck() && !read.isUnmapped())
                    .mapPartitions(readItr ->
                            new MapPartitioner<>(readItr,
                                    new QNameFinder(maxQnamesPerInterval,
                                                    broadcastMetadata.value(),
                                                    broadcastIntervals.value())), false)
                    .collect();
        broadcastIntervals.destroy();

        final HopscotchHashSet<QNameAndInterval> qNames =
                new HopscotchHashSet<>(qNameAndIntervalList, ASSEMBLY_TO_MAPPED_SIZE_RATIO);

        // remove intervals that have ridiculously high coverage -- these are mapper artifacts
        cleanQNames(qNames, intervals, meanBasesPerTemplate);

        // record the qnames with their interval IDs
        if ( locations.qNamesMappedFile != null ) {
            dumpQNames(locations.qNamesMappedFile, pipelineOptions, qNames);
        }

        log("Discovered " + qNames.size() + " mapped template names.");
        return qNames;
    }

    private static void dumpQNames( final String qNameFile,
                                    final PipelineOptions pipelineOptions,
                                    final Collection<QNameAndInterval> qNames ) {
        try (final OutputStreamWriter writer = new OutputStreamWriter(new BufferedOutputStream(
                BucketUtils.createFile(qNameFile, pipelineOptions)))) {
            for (final QNameAndInterval qnameAndInterval : qNames) {
                writer.write(qnameAndInterval.toString() + "\n");
            }
        } catch (final IOException ioe) {
            throw new GATKException("Can't write qname intervals file " + qNameFile, ioe);
        }
    }

    /** remove all qnames for intervals that have ridiculously high coverage */
    private static void cleanQNames( final Collection<QNameAndInterval> qNames,
                                     final List<Interval> intervals,
                                     final int meanBasesPerTemplate ) {

        // find the number of qnames for each interval
        final int nIntervals = intervals.size();
        final int[] qNamesPerInterval = new int[nIntervals];
        for ( final QNameAndInterval qNameAndInterval : qNames ) {
            qNamesPerInterval[qNameAndInterval.getIntervalId()] += 1;
        }

        // for each interval, figure out how many qnames are too many (it depends on read length and interval size)
        // mark the interval for deletion if the coverage is too high
        int nKilled = 0;
        for ( int idx = 0; idx != nIntervals; ++idx ) {
            final int nNamesForExcessiveCoverage = MAX_COVERAGE*intervals.get(idx).getLength()/meanBasesPerTemplate;
            if ( qNamesPerInterval[idx] > nNamesForExcessiveCoverage ) {
                qNamesPerInterval[idx] = -1;
                nKilled += 1;
            }
        }

        // remove the bad intervals
        final Iterator<QNameAndInterval> itr = qNames.iterator();
        while ( itr.hasNext() ) {
            final QNameAndInterval qNameAndInterval = itr.next();
            if ( qNamesPerInterval[qNameAndInterval.getIntervalId()] == -1 ) itr.remove();
        }

        log("Killed " + nKilled + " intervals that had >" + MAX_COVERAGE + "x coverage.");
    }

    /**
     * Identify funky reads that support a hypothesis of a breakpoint in the vicinity, group the reads,
     * and declare a breakpoint interval where there is sufficient density of evidence. */
    private static List<Interval> getIntervals( final Broadcast<ReadMetadata> broadcastMetadata,
                                                final SAMFileHeader header,
                                                final JavaRDD<GATKRead> reads,
                                                final Locations locations,
                                                final PipelineOptions pipelineOptions ) {
        // find all breakpoint evidence, then filter for pile-ups
        final int maxFragmentSize = broadcastMetadata.value().getMaxMedianFragmentSize();
        final List<SAMSequenceRecord> contigs = header.getSequenceDictionary().getSequences();
        final int nContigs = contigs.size();
        final JavaRDD<BreakpointEvidence> evidenceRDD =
                reads
                    .filter(read -> !read.isDuplicate() && !read.failsVendorQualityCheck() && !read.isUnmapped() &&
                            read.getMappingQuality() >= MIN_MAPQ &&
                            read.getCigar().getCigarElements()
                                    .stream()
                                    .filter(ele -> ele.getOperator()==CigarOperator.MATCH_OR_MISMATCH)
                                    .mapToInt(CigarElement::getLength)
                                    .sum() >= MIN_MATCH_LEN)
                    .mapPartitions(readItr ->
                            new MapPartitioner<>(readItr, new ReadClassifier(broadcastMetadata.value())), true)
                    .mapPartitions(evidenceItr ->
                            new MapPartitioner<>(evidenceItr, new BreakpointClusterer(2*maxFragmentSize)), true)
                    .mapPartitions(evidenceItr ->
                            new MapPartitioner<>(evidenceItr,
                                    new WindowSorter(3*maxFragmentSize), new BreakpointEvidence(nContigs)), true);

        // record the evidence
        if ( locations.evidenceDir != null ) {
            evidenceRDD.cache();
            evidenceRDD.saveAsTextFile(locations.evidenceDir);
        }

        // find discrete intervals that contain the breakpoint evidence
        final Iterator<Interval> intervalItr =
                evidenceRDD
                        .mapPartitions(evidenceItr ->
                                new MapPartitioner<>(evidenceItr,
                                        new IntervalMapper(maxFragmentSize), new BreakpointEvidence(nContigs)), true)
                        .collect()
                        .iterator();

        // coalesce overlapping intervals (can happen at partition boundaries)
        final List<Interval> intervals = new ArrayList<>();
        if ( intervalItr.hasNext() ) {
            Interval prev = intervalItr.next();
            while ( intervalItr.hasNext() ) {
                final Interval next = intervalItr.next();
                if ( prev.isDisjointFrom(next) ) {
                    intervals.add(prev);
                    prev = next;
                } else {
                    prev = prev.join(next);
                }
            }
            intervals.add(prev);
        }

        if ( locations.evidenceDir != null ) evidenceRDD.unpersist();

        // record the intervals
        if ( locations.intervalFile != null ) {
            try (final OutputStreamWriter writer = new OutputStreamWriter(new BufferedOutputStream(
                    BucketUtils.createFile(locations.intervalFile, pipelineOptions)))) {
                for (final Interval interval : intervals) {
                    final String seqName = contigs.get(interval.getContig()).getSequenceName();
                    writer.write(seqName + " " + interval.getStart() + " " + interval.getEnd() + "\n");
                }
            } catch (final IOException ioe) {
                throw new GATKException("Can't write intervals file " + locations.intervalFile, ioe);
            }
        }

        log("Discovered "+intervals.size()+" intervals.");
        return intervals;
    }

    /** gather some interesting factoids about the reads in aggregate */
    private static ReadMetadata getMetadata( final SAMFileHeader header,
                                             final int meanBasesPerTemplate ) {
        final List<SAMReadGroupRecord> groups = header.getReadGroups();
        final int nGroups = groups.size();
        final ReadMetadata.ReadGroupFragmentStatistics groupStats =
                //TODO: get real data
                new ReadMetadata.ReadGroupFragmentStatistics(400.f, 75.f);
        final List<ReadMetadata.ReadGroupFragmentStatistics> stats = new ArrayList<>(nGroups);
        for ( int idx = 0; idx != nGroups; ++idx ) {
            stats.add(groupStats);
        }
        final ReadMetadata readMetadata =
                new ReadMetadata(header, stats, groupStats, meanBasesPerTemplate);
        log("Metadata retrieved.");
        return readMetadata;
    }

    /** mean number of sequenced bases per template (i.e., read length if unpaired, twice the read length if paired) */
    private static int getMeanBasesPerTemplate( final JavaRDD<GATKRead> reads ) {
        return reads
                .map(ReadCountAndLength::new)
                .reduce(ReadCountAndLength::new)
                .getMeanLength();
    }

    private static void log( final String message ) {
        System.out.println(dateFormatter.format(System.currentTimeMillis())+message);
    }

    private static class Locations {
        public final String outputDir;
        public final String evidenceDir;
        public final String intervalFile;
        public final String qNamesMappedFile;
        public final String kmerFile;
        public final String qNamesAssemblyFile;
        public final String kmersToIgnoreFilename;

        public Locations( final String outputDir, final String evidenceDir, final String intervalFile,
                          final String qNamesMappedFile, final String kmerFile, final String qNamesAssemblyFile,
                          final String kmersToIgnoreFilename ) {
            this.outputDir = outputDir;
            this.evidenceDir = evidenceDir;
            this.intervalFile = intervalFile;
            this.qNamesMappedFile = qNamesMappedFile;
            this.kmerFile = kmerFile;
            this.qNamesAssemblyFile = qNamesAssemblyFile;
            this.kmersToIgnoreFilename = kmersToIgnoreFilename;
        }
    }

    /** helper class for calculating mean template length */
    @DefaultSerializer(ReadCountAndLength.Serializer.class)
    private static final class ReadCountAndLength {
        private final long count;
        private final long length;

        ReadCountAndLength( final GATKRead read ) {
            this.count = 1;
            this.length = (read.isPaired() ? 2 : 1)*read.getLength();
        }

        ReadCountAndLength( final ReadCountAndLength countNLength1, final ReadCountAndLength countNLength2 ) {
            this.count = countNLength1.count + countNLength2.count;
            this.length = countNLength1.length + countNLength2.length;
        }

        private ReadCountAndLength( final Kryo kryo, final Input input ) {
            count = input.readLong();
            length = input.readLong();
        }

        private void serialize( final Kryo kryo, final Output output ) {
            output.writeLong(count);
            output.writeLong(length);
        }

        public int getMeanLength() { return count != 0 ? (int)(length/count) : 0; }

        public static final class Serializer extends com.esotericsoftware.kryo.Serializer<ReadCountAndLength> {
            @Override
            public void write( final Kryo kryo, final Output output, final ReadCountAndLength readCountAndLength ) {
                readCountAndLength.serialize(kryo, output);
            }

            @Override
            public ReadCountAndLength read( final Kryo kryo, final Input input,
                                            final Class<ReadCountAndLength> klass ) {
                return new ReadCountAndLength(kryo, input);
            }
        }
    }

    /**
     * A class that acts as a filter for breakpoint evidence.
     * It passes only that evidence that is part of a putative cluster.
     */
    private static final class BreakpointClusterer
            implements Function<BreakpointEvidence, Iterator<BreakpointEvidence>> {
        private final int staleEventDistance;
        private final SortedMap<BreakpointEvidence, Boolean> locMap = new TreeMap<>();
        private final List<Map.Entry<BreakpointEvidence, Boolean>> reportableEntries = new ArrayList<>(2*MIN_EVIDENCE);
        private final Iterator<BreakpointEvidence> noEvidence = Collections.emptyIterator();
        private int currentContig = -1;

        private static final int MIN_EVIDENCE = 15; // minimum evidence count in a cluster

        BreakpointClusterer( final int staleEventDistance ) {
            this.staleEventDistance = staleEventDistance;
        }

        public Iterator<BreakpointEvidence> apply( final BreakpointEvidence evidence ) {
            if ( evidence.getContigIndex() != currentContig ) {
                currentContig = evidence.getContigIndex();
                locMap.clear();
            }

            locMap.put(evidence, true);

            final int locusStart = evidence.getContigStart();
            final int locusEnd = evidence.getContigEnd();
            final int staleEnd = locusStart - staleEventDistance;
            int evidenceCount = 0;
            reportableEntries.clear();
            final Iterator<Map.Entry<BreakpointEvidence, Boolean>> itr = locMap.entrySet().iterator();
            while ( itr.hasNext() ) {
                final Map.Entry<BreakpointEvidence, Boolean> entry = itr.next();
                final BreakpointEvidence evidence2 = entry.getKey();
                final int contigEnd = evidence2.getContigEnd();
                if ( contigEnd <= staleEnd ) itr.remove();
                else if ( evidence2.getContigStart() >= locusEnd ) break;
                else if ( contigEnd > locusStart ) {
                    evidenceCount += 1;
                    if ( entry.getValue() ) reportableEntries.add(entry);
                }
            }

            if ( evidenceCount >= MIN_EVIDENCE ) {
                return reportableEntries.stream()
                        .map(entry -> { entry.setValue(false); return entry.getKey(); })
                        .iterator();
            }
            return noEvidence;
        }
    }

    /**
     * Class to fully sort a stream of nearly sorted BreakpointEvidences.
     */
    private static final class WindowSorter
            implements Function<BreakpointEvidence, Iterator<BreakpointEvidence>> {
        private final SortedSet<BreakpointEvidence> recordSet = new TreeSet<>();
        private final List<BreakpointEvidence> reportableEvidence = new ArrayList<>();
        private final int windowSize;
        private int currentContig = -1;

        WindowSorter( final int windowSize ) {
            this.windowSize = windowSize;
        }

        public Iterator<BreakpointEvidence> apply( final BreakpointEvidence evidence ) {
            reportableEvidence.clear();
            if ( evidence.getContigIndex() != currentContig ) {
                reportableEvidence.addAll(recordSet);
                recordSet.clear();
                currentContig = evidence.getContigIndex();
            } else {
                final int reportableEnd = evidence.getContigStart() - windowSize;
                final Iterator<BreakpointEvidence> itr = recordSet.iterator();
                while ( itr.hasNext() ) {
                    final BreakpointEvidence evidence2 = itr.next();
                    if ( evidence2.getContigStart() >= reportableEnd ) break;
                    reportableEvidence.add(evidence2);
                    itr.remove();
                }
            }
            recordSet.add(evidence);
            return reportableEvidence.iterator();
        }
    }

    /**
     * Minimalistic simple interval.
     */
    @DefaultSerializer(Interval.Serializer.class)
    private static final class Interval {
        private final int contig;
        private final int start;
        private final int end;

        Interval( final int contig, final int start, final int end ) {
            this.contig = contig;
            this.start = start;
            this.end = end;
        }

        private Interval( final Kryo kryo, final Input input ) {
            contig = input.readInt();
            start = input.readInt();
            end = input.readInt();
        }

        private void serialize( final Kryo kryo, final Output output ) {
            output.writeInt(contig);
            output.writeInt(start);
            output.writeInt(end);
        }

        public int getContig() { return contig; }
        public int getStart() { return start; }
        public int getEnd() { return end; }
        public int getLength() { return end-start; }

        public boolean isDisjointFrom( final Interval that ) {
            return this.contig != that.contig || this.end < that.start || that.end < this.start;
        }

        public Interval join( final Interval that ) {
            if ( this.contig != that.contig ) throw new GATKException("Joining across contigs.");
            return new Interval(contig, Math.min(this.start, that.start), Math.max(this.end, that.end));
        }

        public static final class Serializer extends com.esotericsoftware.kryo.Serializer<Interval> {
            @Override
            public void write( final Kryo kryo, final Output output, final Interval interval ) {
                interval.serialize(kryo, output);
            }

            @Override
            public Interval read( final Kryo kryo, final Input input, final Class<Interval> klass ) {
                return new Interval(kryo, input);
            }
        }
    }

    /**
     * A class to examine a stream of BreakpointEvidence, and group it into Intervals.
     */
    private static final class IntervalMapper implements Function<BreakpointEvidence, Iterator<Interval>> {
        private final Iterator<Interval> noInterval = Collections.emptyIterator();
        private final int gapSize;
        private int contig = -1;
        private int start;
        private int end;

        IntervalMapper( final int gapSize ) {
            this.gapSize = gapSize;
        }

        public Iterator<Interval> apply( final BreakpointEvidence evidence ) {
            Iterator<Interval> result = noInterval;
            if ( evidence.getContigIndex() != contig ) {
                if ( contig != -1 ) {
                    result = new SVUtils.SingletonIterator<>(new Interval(contig, start, end));
                }
                contig = evidence.getContigIndex();
                start = evidence.getContigStart();
                end = evidence.getContigEnd();
            } else if ( evidence.getContigStart() >= end+gapSize ) {
                result = new SVUtils.SingletonIterator<>(new Interval(contig, start, end));
                start = evidence.getContigStart();
                end = evidence.getContigEnd();
            } else {
                end = Math.max(end, evidence.getContigEnd());
            }
            return result;
        }
    }

    /**
     * A template name and an intervalId.
     * Note:  hashCode does not depend on intervalId, and that's on purpose.
     * This is actually a compacted (K,V) pair, and the hashCode is on K only.
     */
    @DefaultSerializer(QNameAndInterval.Serializer.class)
    private static final class QNameAndInterval {
        private final byte[] qName;
        private final int hashVal;
        private final int intervalId;

        QNameAndInterval( final String qName, final int intervalId ) {
            this.qName = qName.getBytes();
            this.hashVal = qName.hashCode();
            this.intervalId = intervalId;
        }

        private QNameAndInterval( final Kryo kryo, final Input input ) {
            final int nameLen = input.readInt();
            qName = input.readBytes(nameLen);
            hashVal = input.readInt();
            intervalId = input.readInt();
        }

        private void serialize( final Kryo kryo, final Output output ) {
            output.writeInt(qName.length);
            output.writeBytes(qName);
            output.writeInt(hashVal);
            output.writeInt(intervalId);
        }

        public String getQName() { return new String(qName); }
        public int getIntervalId() { return intervalId; }

        @Override
        public int hashCode() { return hashVal; }

        @Override
        public boolean equals( final Object obj ) {
            return obj instanceof QNameAndInterval && equals((QNameAndInterval) obj);
        }

        public boolean equals( final QNameAndInterval that ) {
            return this.intervalId == that.intervalId && Arrays.equals(this.qName, that.qName);
        }

        public boolean sameName( final byte[] name ) { return Arrays.equals(qName, name); }

        public String toString() { return new String(qName)+" "+intervalId; }

        public static final class Serializer extends com.esotericsoftware.kryo.Serializer<QNameAndInterval> {
            @Override
            public void write( final Kryo kryo, final Output output, final QNameAndInterval qNameAndInterval ) {
                qNameAndInterval.serialize(kryo, output);
            }

            @Override
            public QNameAndInterval read( final Kryo kryo, final Input input, final Class<QNameAndInterval> klass ) {
                return new QNameAndInterval(kryo, input);
            }
        }
    }

    /**
     * Class to find the template names associated with reads in specified intervals.
     */
    private static final class QNameFinder implements Function<GATKRead, Iterator<QNameAndInterval>> {
        private final int maxQnamesPerInterval;
        private final ReadMetadata metadata;
        private final List<Interval> intervals;
        private final int[] qNamesPerInterval;
        private final Iterator<QNameAndInterval> noName = Collections.emptyIterator();
        private int intervalsIndex = 0;

        QNameFinder( final int maxQnamesPerInterval, final ReadMetadata metadata, final List<Interval> intervals ) {
            this.maxQnamesPerInterval = maxQnamesPerInterval;
            this.metadata = metadata;
            this.intervals = intervals;
            qNamesPerInterval = new int[intervals.size()];
        }

        @Override
        public Iterator<QNameAndInterval> apply( final GATKRead read ) {
            final int readContigId = metadata.getContigID(read.getContig());
            final int readStart = read.getUnclippedStart();
            final int intervalsSize = intervals.size();
            while ( intervalsIndex < intervalsSize ) {
                final Interval interval = intervals.get(intervalsIndex);
                if ( interval.getContig() > readContigId ) break;
                if ( interval.getContig() == readContigId && interval.getEnd() > read.getStart() ) break;
                intervalsIndex += 1;
            }
            if ( intervalsIndex >= intervalsSize ) return noName;
            final Interval indexedInterval = intervals.get(intervalsIndex);
            final Interval readInterval = new Interval(readContigId, readStart, read.getUnclippedEnd());
            if ( indexedInterval.isDisjointFrom(readInterval) ) return noName;
            if ( qNamesPerInterval[intervalsIndex]++ > maxQnamesPerInterval ) return noName;
            return new SVUtils.SingletonIterator<>(new QNameAndInterval(read.getName(), intervalsIndex));
        }
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

        public int getIntervalId() { return intervalId; }

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
     * Class that acts as a mapper from a stream of reads to a stream of KmerAndIntervals.
     * The template names of reads to kmerize, along with a set of kmers to ignore are passed in (by broadcast).
     */
    private static final class QNameKmerizer implements Function<GATKRead, Iterator<Tuple2<KmerAndInterval, Integer>>> {
        private final HopscotchHashSet<QNameAndInterval> qNameAndIntervalSet;
        private final Set<SVKmer> kmersToIgnore;
        private final ArrayList<Tuple2<KmerAndInterval, Integer>> tupleList = new ArrayList<>();

        QNameKmerizer( final HopscotchHashSet<QNameAndInterval> qNameAndIntervalSet,
                       final Set<SVKmer> kmersToIgnore ) {
            this.qNameAndIntervalSet = qNameAndIntervalSet;
            this.kmersToIgnore = kmersToIgnore;
        }

        public Iterator<Tuple2<KmerAndInterval, Integer>> apply( final GATKRead read ) {
            final String qName = read.getName();
            final int qNameHash = qName.hashCode();
            final byte[] qNameBytes = qName.getBytes();
            final Iterator<QNameAndInterval> names = qNameAndIntervalSet.bucketIterator(qNameHash);
            tupleList.clear();
            while ( names.hasNext() ) {
                final QNameAndInterval qNameAndInterval = names.next();
                if ( qNameAndInterval.hashCode() == qNameHash && qNameAndInterval.sameName(qNameBytes) ) {
                    SVKmerizer.stream(read.getBases(), SVConstants.KMER_SIZE)
                            .map(kmer -> kmer.canonical(SVConstants.KMER_SIZE))
                            .filter(kmer -> !kmersToIgnore.contains(kmer))
                            .map(kmer -> new KmerAndInterval(kmer, qNameAndInterval.getIntervalId()))
                            .forEach(kmerCountAndInterval -> tupleList.add(new Tuple2<>(kmerCountAndInterval,1)));
                    break;
                }
            }
            return tupleList.iterator();
        }
    }

    /**
     * Eliminates dups, and removes over-represented kmers.
     */
    private static final class KmerCleaner {
        private static final int MAX_INTERVALS = 3;
        private static final int MIN_KMER_COUNT = 3;
        private static final int MAX_KMER_COUNT = 125;
        private static final int MAX_KMERS_PER_REHASHED_PARTITION_GUESS = 600000;

        private final HopscotchHashSet<KmerAndInterval> kmerSet =
                new HopscotchHashSet<>(MAX_KMERS_PER_REHASHED_PARTITION_GUESS);

        public Iterable<KmerAndInterval> call(final Iterator<Tuple2<KmerAndInterval, Integer>> kmerCountItr ) {

            // remove kmers with extreme counts that won't help in building a local assembly
            while ( kmerCountItr.hasNext() ) {
                final Tuple2<KmerAndInterval, Integer> kmerCount = kmerCountItr.next();
                final int count = kmerCount._2;
                if ( count >= MIN_KMER_COUNT && count <= MAX_KMER_COUNT ) kmerSet.add(kmerCount._1);
            }

            // remove ubiquitous kmers that appear in too many intervals
            final int kmerSetCapacity = kmerSet.capacity();
            for ( int idx = 0; idx != kmerSetCapacity; ++idx ) {
                cleanBucket(idx);
            }

            return kmerSet;
        }

        /** clean up ubiquitous kmers that hashed to some given hash bucket */
        private void cleanBucket( final int bucketIndex ) {
            final int nKmers = SVUtils.iteratorSize(kmerSet.bucketIterator(bucketIndex));
            if ( nKmers <= MAX_INTERVALS ) return;

            // create an array of all the entries in the given bucket
            final KmerAndInterval[] kmers = new KmerAndInterval[nKmers];
            final Iterator<KmerAndInterval> itr = kmerSet.bucketIterator(bucketIndex);
            int idx = 0;
            while ( itr.hasNext() ) {
                kmers[idx++] = itr.next();
            }

            // sort the entries by kmer
            Arrays.sort(kmers);

            // remove entries sharing a kmer value that appear in more than MAX_INTERVALS intervals
            // readIdx starts a group, and testIdx is bumped until the kmer changes (ignoring interval id)
            int readIdx = 0;
            int testIdx = 1;
            while ( testIdx < nKmers ) {
                // first kmer in the group
                final SVKmer test = kmers[readIdx];

                // bump testIdx until the kmer changes (or we run out of entries)
                while ( testIdx < nKmers && test.equals(kmers[testIdx]) ) {
                    ++testIdx;
                }

                // if the number of entries with the same kmer is too big
                if ( testIdx-readIdx > MAX_INTERVALS ) {
                    // kill 'em
                    while ( readIdx != testIdx ) {
                        kmerSet.remove(kmers[readIdx++]);
                    }
                }
                readIdx = testIdx;
                testIdx += 1;
            }
        }
    }

    /**
     * Class that acts as a mapper from a stream of reads to a stream of <intervalId,read> pairs.
     * It knows which breakpoint(s) a read belongs to (if any) by kmerizing the read, and looking up each SVKmer in
     * a multi-map of SVKmers onto intervalId.
     */
    private static final class QNamesForKmersFinder implements Function<GATKRead, Iterator<QNameAndInterval>> {
        private final HopscotchHashSet<KmerAndInterval> kmerAndIntervalSet;
        private final Set<Integer> intervalIdSet = new HashSet<>();
        private final List<QNameAndInterval> qNameAndIntervalList = new ArrayList<>();
        private final Iterator<QNameAndInterval> emptyIterator = Collections.emptyIterator();

        QNamesForKmersFinder(final HopscotchHashSet<KmerAndInterval> kmerAndIntervalSet) {
            this.kmerAndIntervalSet = kmerAndIntervalSet;
        }

        public Iterator<QNameAndInterval> apply(final GATKRead read) {
            intervalIdSet.clear();
            SVKmerizer.stream(read.getBases(), SVConstants.KMER_SIZE)
                    .map( kmer -> kmer.canonical(SVConstants.KMER_SIZE) )
                    .forEach( kmer -> {
                        final Iterator<KmerAndInterval> itr = kmerAndIntervalSet.bucketIterator(kmer.hashCode());
                        while ( itr.hasNext() ) {
                            final KmerAndInterval kmerAndInterval = itr.next();
                            if (kmer.equals(kmerAndInterval)) {
                                intervalIdSet.add(kmerAndInterval.getIntervalId());
                            }
                        }
                    });
            if (intervalIdSet.isEmpty()) return emptyIterator;
            qNameAndIntervalList.clear();
            final String qName = read.getName();
            intervalIdSet.stream().forEach(intervalId -> qNameAndIntervalList.add(new QNameAndInterval(qName, intervalId)));
            return qNameAndIntervalList.iterator();
        }
    }

    /**
     * Find <intervalId,fastqBytes> pairs for interesting template names.
     */
    private static final class ReadsForQNamesFinder {
        private final HopscotchHashSet<QNameAndInterval> qNamesSet;
        private final int nIntervals;
        private final int nReadsPerInterval;
        private final List<Tuple2<Integer, List<byte[]>>> fastQRecords = new ArrayList<>();

        @SuppressWarnings("unchecked")
        ReadsForQNamesFinder( final HopscotchHashSet<QNameAndInterval> qNamesSet, final int nIntervals ) {
            this.qNamesSet = qNamesSet;
            this.nIntervals = nIntervals;
            this.nReadsPerInterval = 2*qNamesSet.size()/nIntervals;
        }

        public Iterable<Tuple2<Integer, List<byte[]>>> call( final Iterator<GATKRead> readsItr ) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            final List<byte[]>[] intervalReads = new List[nIntervals];
            int nPopulatedIntervals = 0;
            while ( readsItr.hasNext() ) {
                final GATKRead read = readsItr.next();
                final String readName = read.getName();
                final int readNameHash = readName.hashCode();
                final byte[] readNameBytes = readName.getBytes();
                final Iterator<QNameAndInterval> namesItr = qNamesSet.bucketIterator(readNameHash);
                byte[] fastqBytes = null;
                while (namesItr.hasNext()) {
                    final QNameAndInterval nameAndInterval = namesItr.next();
                    if (nameAndInterval.hashCode() == readNameHash && nameAndInterval.sameName(readNameBytes)) {
                        if (fastqBytes == null) fastqBytes = fastqForRead(read).getBytes();
                        final int intervalId = nameAndInterval.getIntervalId();
                        if ( intervalReads[intervalId] == null ) {
                            intervalReads[intervalId] = new ArrayList<>(nReadsPerInterval);
                            nPopulatedIntervals += 1;
                        }
                        intervalReads[intervalId].add(fastqBytes);
                    }
                }
            }
            fastQRecords.clear();
            if ( nPopulatedIntervals > 0 ) {
                for ( int idx = 0; idx != nIntervals; ++idx ) {
                    final List<byte[]> readList = intervalReads[idx];
                    if ( readList != null ) fastQRecords.add(new Tuple2<>(idx, readList));
                }
            }
            return fastQRecords;
        }

        private String fastqForRead( final GATKRead read ) {
            final String nameSuffix = read.isPaired() ? (read.isFirstOfPair() ? "/1" : "/2") : "";
            //final String mappedLocation = read.isUnmapped() ? "*" : read.getContig()+":"+read.getStart();
            return "@" + read.getName() + nameSuffix +
                    // "|" + mappedLocation +
                    "\n" +
                    read.getBasesString() + "\n" +
                    "+\n" +
                    ReadUtils.getBaseQualityString(read)+"\n";
        }
    }
}
