package org.broadinstitute.hellbender.tools.spark.sv;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import htsjdk.samtools.fastq.FastqReader;
import htsjdk.samtools.fastq.FastqRecord;
import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.utils.BaseUtils;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import scala.Tuple2;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * To make this test runnable, "-DPath.To.SGA" should be set properly.
 */
// TODO: investigate why SGA, run with exactly the same parameters on the same machine, yields slightly different results: either SNP between the contigs or slightly different contig lengths
// TODO: test if error messages are handled correctly
public class RunSGAViaProcessBuilderOnSparkUnitTest extends CommandLineProgramTest {

    private static final Path sgaPath = getSGAPath();
    private static Path getSGAPath(){
        String s = System.getProperty("Path.To.SGA");
        if(null==s){
            s = System.getenv("Path.To.SGA");
        }
        return (null==s) ? null : Paths.get(s);
    }

    private static final SGAModule indexer = new SGAModule("index");

    // data block
    private static final File TEST_DATA_DIR = new File(getTestDataDir(), "spark/sv/RunSGAViaProcessBuilderOnSpark/");

    private static final List<Tuple2<Long, URI>> rawFASTQFiles;
    private static final Map<Long, URI> expectedAssembledFASTAFiles;
    static {
        rawFASTQFiles = new ArrayList<>();
        expectedAssembledFASTAFiles = new HashMap<>();

        Long i = 0L;

        final String[] extension = new String[1];
        extension[0] = "fastq";

        final Iterator<File> it = FileUtils.iterateFiles(TEST_DATA_DIR, extension, false);

        while(it.hasNext()){

            final String fastqFileName = it.next().getAbsolutePath().toString();
            final Tuple2<Long, URI> breakpoint = RunSGAViaProcessBuilderOnSpark.assignFASTQToBreakpoints(String.valueOf(i) + "\t" + Paths.get(fastqFileName).toUri());
            rawFASTQFiles.add(breakpoint);

            final String contigFileName = fastqFileName.replace("fastq", "pp.ec.filter.pass.rmdup.merged-contigs.fa");
            expectedAssembledFASTAFiles.put(i++, Paths.get(contigFileName).toUri());
        }
    }


    // parameter block
    private static final int editDistanceTolerance = 2; // tolerance of the summed edit distance between expected contigs and corresponding actual assembled contigs

    private static final int threads = 1;

    @BeforeMethod
    public void checkPATHTOSGAAvailability(){
        if(null==sgaPath){
            throw new SkipException("Skipping test because \"-DPath.To.SGA\" is set neither in system property nor as an environment variable.");
        }
    }

    @Test(groups = "sv")
    public void assemblyOneStepTest() throws IOException, InterruptedException, RuntimeException{

        for(final Tuple2<Long, URI> breakpoint : rawFASTQFiles){

            final RunSGAViaProcessBuilderOnSpark.SGAAssemblyResult result = RunSGAViaProcessBuilderOnSpark.performAssembly(breakpoint, sgaPath, threads, true)._2();

            final File expectedAssembledContigFile = new File( expectedAssembledFASTAFiles.get(breakpoint._1()) );
            final RunSGAViaProcessBuilderOnSpark.ContigsCollection expectedContigsCollection = new RunSGAViaProcessBuilderOnSpark.ContigsCollection(expectedAssembledContigFile);

            Assert.assertTrue( compareContigs(result.assembledContigs, expectedContigsCollection) );
        }
    }

    @Test(groups = "sv")
    public void assemblyStepByStepTest() throws IOException, InterruptedException, RuntimeException{

        for(final Tuple2<Long, URI> breakpoint : rawFASTQFiles){

            // make temp dir, copy file
            final File workingDir = Files.createTempDirectory("whatever").toAbsolutePath().toFile();
            workingDir.deleteOnExit();

            final String pathStringToRawFASTQ = breakpoint._2().getPath();
            final String rawFASTQFileName = FilenameUtils.getName(pathStringToRawFASTQ);
            BucketUtils.copyFile(pathStringToRawFASTQ, null, workingDir.toPath().toAbsolutePath().toString()+"/"+rawFASTQFileName);

            final File fastqFile = new File(workingDir, FilenameUtils.getName(breakpoint._2().toString()));
            final File expectedAssembledContigFile = new File( expectedAssembledFASTAFiles.get(breakpoint._1()) );

            stepByStepTestWorker(workingDir, fastqFile, expectedAssembledContigFile);
        }
    }

    private static void stepByStepTestWorker(final File workingDir, final File rawFASTQ, final File expectedAssembledContigFile) throws IOException, InterruptedException, RuntimeException{

        ArrayList<Integer> editDistancesBetweenSeq = new ArrayList<>();
        final Integer zero = 0;

        final ArrayList<String> indexerArgs = new ArrayList<>();
        indexerArgs.add("--algorithm"); indexerArgs.add("ropebwt");
        indexerArgs.add("--threads");   indexerArgs.add(Integer.toString(threads));
        indexerArgs.add("--check");
        indexerArgs.add("");

        final String filenamePrefix = FilenameUtils.getBaseName(rawFASTQ.getName());

        final File actualPreppedFile = new File(workingDir, RunSGAViaProcessBuilderOnSpark.runSGAPreprocess(sgaPath, rawFASTQ, workingDir, indexer, indexerArgs).fileNameToReturn);
        final File expectedPreppedFile = new File(TEST_DATA_DIR, filenamePrefix + ".pp.fa");
        final String preppedFileName = compareNamesAndComputeSeqEditDist(actualPreppedFile, expectedPreppedFile, true, editDistancesBetweenSeq);
        for(final Integer d : editDistancesBetweenSeq){ Assert.assertEquals(d, zero); }

        editDistancesBetweenSeq = new ArrayList<>();
        final File preppedFile = new File(workingDir, preppedFileName);
        final File actualCorrectedFile = new File(workingDir, RunSGAViaProcessBuilderOnSpark.runSGACorrect(sgaPath, preppedFile, workingDir, threads, indexer, indexerArgs).fileNameToReturn);
        final File expectedCorrectedFile = new File(TEST_DATA_DIR, filenamePrefix + ".pp.ec.fa");
        final String correctedFileName = compareNamesAndComputeSeqEditDist(actualCorrectedFile, expectedCorrectedFile, true, editDistancesBetweenSeq);
        for(final Integer d : editDistancesBetweenSeq){ Assert.assertEquals(d, zero); }

        editDistancesBetweenSeq = new ArrayList<>();
        final File correctedFile = new File(workingDir, correctedFileName);
        final File actualFilterPassingFile = new File(workingDir, RunSGAViaProcessBuilderOnSpark.runSGAFilter(sgaPath, correctedFile, workingDir, threads).fileNameToReturn);
        final File expectedFilterPassingFile = new File(TEST_DATA_DIR, filenamePrefix + ".pp.ec.filter.pass.fa");
        final String filterPassingFileName = compareNamesAndComputeSeqEditDist(actualFilterPassingFile, expectedFilterPassingFile, true, editDistancesBetweenSeq);
        for(final Integer d : editDistancesBetweenSeq){ Assert.assertEquals(d, zero); }

        editDistancesBetweenSeq = new ArrayList<>();
        final File filterPassingFile = new File(workingDir, filterPassingFileName);
        final File actualRmdupFile = new File(workingDir, RunSGAViaProcessBuilderOnSpark.runSGARmDuplicate(sgaPath, filterPassingFile, workingDir, threads, indexer, indexerArgs).fileNameToReturn);
        final File expectedRmdupFile = new File(TEST_DATA_DIR, filenamePrefix + ".pp.ec.filter.pass.rmdup.fa");
        final String duplicateRemovedFileName = compareNamesAndComputeSeqEditDist(actualRmdupFile, expectedRmdupFile, false, editDistancesBetweenSeq);
        for(final Integer d : editDistancesBetweenSeq){ Assert.assertEquals(d, zero); }

        editDistancesBetweenSeq = new ArrayList<>();
        final File duplicateRemovedFile = new File(workingDir, duplicateRemovedFileName);
        final File actualMergedFile = new File(workingDir, RunSGAViaProcessBuilderOnSpark.runSGAFMMerge(sgaPath, duplicateRemovedFile, workingDir, threads, indexer, indexerArgs).fileNameToReturn);
        final File expectedMergedFile = new File(TEST_DATA_DIR, filenamePrefix + ".pp.ec.filter.pass.rmdup.merged.fa");
        final String mergedFileName = compareNamesAndComputeSeqEditDist(actualMergedFile, expectedMergedFile, false, editDistancesBetweenSeq);
        for(final Integer d : editDistancesBetweenSeq){ Assert.assertEquals(d, zero); }

        // final assembled contig test
        final File mergedFile = new File(workingDir, mergedFileName);
        final File actualAssembledContigsFile = new File(workingDir, RunSGAViaProcessBuilderOnSpark.runSGAOverlapAndAssemble(sgaPath, mergedFile, workingDir, threads).fileNameToReturn);
        final RunSGAViaProcessBuilderOnSpark.ContigsCollection actualAssembledContigs = new RunSGAViaProcessBuilderOnSpark.ContigsCollection(actualAssembledContigsFile);
        final RunSGAViaProcessBuilderOnSpark.ContigsCollection expectedAssembledContigs = new RunSGAViaProcessBuilderOnSpark.ContigsCollection(expectedAssembledContigFile);
        Assert.assertTrue( compareContigs(actualAssembledContigs, expectedAssembledContigs) );
    }

    private static boolean compareContigs(final RunSGAViaProcessBuilderOnSpark.ContigsCollection actualAssembledContigs,
                                          final RunSGAViaProcessBuilderOnSpark.ContigsCollection expectedAssembledContigs)
            throws IOException, InterruptedException{

        final SortedMap<Integer, String> expectedMap = collectContigsByLength(expectedAssembledContigs);

        // first create mapping from read length to sequence
        // (bad, but contig names are not guaranteed to be reproducible in different runs)
        final SortedMap<Integer, String> actualMap = collectContigsByLength(actualAssembledContigs);


        // then compare sequences, aware of RC
        final SortedSet<Integer> actualLengthVacs = new TreeSet<>(actualMap.keySet());
        final SortedSet<Integer> expectedLengthVals = new TreeSet<>(expectedMap.keySet());
        Assert.assertEquals(actualLengthVacs, expectedLengthVals);

        boolean result = true;
        for(final Integer l : actualLengthVacs){

            boolean sequencesAreTheSame = false;
            boolean sequencesAreCloseEnough = false;

            final String actualString = actualMap.get(l);
            final String expectedString = expectedMap.get(l);
            final String rcOfActualString = new String(BaseUtils.simpleReverseComplement(actualString.getBytes(StandardCharsets.UTF_8)));

            final int dist = StringUtils.getLevenshteinDistance(actualString, expectedString);
            final int rcDist = StringUtils.getLevenshteinDistance(rcOfActualString, expectedString);

            if(actualString.equals(expectedString) || rcOfActualString.equals(expectedString)){
                sequencesAreCloseEnough = sequencesAreTheSame = true;
            }else{
                // two things: the unit test tests if sequences are similar enough to each other
                // but if the minimum edit distance is different, we want to see which one it is.
                final int minDist = (dist < rcDist ? dist : rcDist);
                sequencesAreCloseEnough = (minDist <= editDistanceTolerance);
                if(0!=minDist){
                    System.err.println("Contig that has nonzero edit distance is of length " + Integer.toString(actualString.length()) +
                                        "\nactual sequence: " + actualString +
                                        "\nreverse complement of actual sequence: " + rcOfActualString +
                                        "\nexpected sequence" + expectedString);
                }
            }

            result &= (sequencesAreTheSame || sequencesAreCloseEnough);
        }
        return result;
    }

    private static String compareNamesAndComputeSeqEditDist(final File actualFile, final File expectedFile, final boolean fastqFilesWellFormed, final ArrayList<Integer> distances) throws IOException{

        final List<String> actualNames = new ArrayList<>();
        final List<String> actualSeq = new ArrayList<>();
        extractNamesAndSeqFromFASTQ(actualFile, fastqFilesWellFormed, actualNames, actualSeq);

        final List<String> expectedNames = new ArrayList<>();
        final List<String> expectedSeq = new ArrayList<>();
        extractNamesAndSeqFromFASTQ(expectedFile, fastqFilesWellFormed, expectedNames, expectedSeq);

        Assert.assertEquals(expectedNames, actualNames);

        // in stead of more stringent tests, e.g. "Assert.assertEquals(expectedSeq, actualSeq);",
        // compute distance and let caller decide how large an edit distance is allowed
        for(int i=0; i<expectedSeq.size(); ++i){
            distances.add(StringUtils.getLevenshteinDistance(expectedSeq.get(i), actualSeq.get(i)));
        }

        //return RunSGAViaProcessBuilderOnSpark.extractBaseNameWithoutExtension(actualFile) + ".fa";
        return FilenameUtils.getBaseName(actualFile.getName()) + ".fa";
    }

    // utility function: for extracting read names and sequences from a fastq file
    private static void extractNamesAndSeqFromFASTQ(final File FASTAFile, final boolean fastqFilesWellFormed, List<String> readNames, List<String> sequences) throws IOException{

        if(fastqFilesWellFormed){
            try(final FastqReader reader = new FastqReader(FASTAFile)){
                while(reader.hasNext()){
                    final FastqRecord record = reader.next();
                    readNames.add(record.getReadHeader());
                    sequences.add(record.getReadString());
                }
            }
        }else{
            try(final BufferedReader reader = new BufferedReader(new FileReader(FASTAFile))){
                String line = "";
                int i=0;
                while ((line = reader.readLine()) != null){
                    final int l = i%4;
                    if(0==l){
                        readNames.add(line);
                        ++i;
                    }else{
                        sequences.add(line);
                        reader.readLine(); reader.readLine();
                        i+=3;
                    }
                }
            }
        }
    }

    // utility function: generate mapping from contig length to its DNA sequence
    // this is possible because test result contigs have unique length values
    private static SortedMap<Integer, String> collectContigsByLength(final RunSGAViaProcessBuilderOnSpark.ContigsCollection contigsCollection){

        final List<Tuple2<RunSGAViaProcessBuilderOnSpark.ContigsCollection.ContigID, RunSGAViaProcessBuilderOnSpark.ContigsCollection.ContigSequence>> sequences = contigsCollection.getContents();
        final Iterator<Tuple2<RunSGAViaProcessBuilderOnSpark.ContigsCollection.ContigID, RunSGAViaProcessBuilderOnSpark.ContigsCollection.ContigSequence>> it = sequences.iterator();

        final SortedMap<Integer, String> result = new TreeMap<>();
        while(it.hasNext()){
            final String seq = it.next()._2().getSequenceAsString();
            result.put(seq.length(), seq);
        }
        return result;
    }
}