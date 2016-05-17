package org.broadinstitute.hellbender.tools.spark.sv;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import htsjdk.samtools.fastq.FastqReader;
import htsjdk.samtools.fastq.FastqRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.utils.BaseUtils;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import scala.Tuple2;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * To make this test runnable, "-Dpath.to.sga" should be set properly.
 */
// TODO: investigate why SGA, run with exactly the same parameters on the same machine, yields slightly different results: either SNP between the contigs or slightly different contig lengths
// TODO: test if error messages are handled correctly
public class RunSGAViaProcessBuilderOnSparkUnitTest extends CommandLineProgramTest {

    private static Path sgaPath;

    private static final SGAModule indexer = new SGAModule("index");

    // data block
    private static final File TEST_DATA_DIR = new File(getTestDataDir(), "spark/sv/RunSGAViaProcessBuilderOnSpark/");

    private static List<Tuple2<Long, URI>> rawFASTQFiles;
    private static Map<Long, URI> expectedAssembledFASTAFiles;

    // parameter block
    private static final int lengthDiffTolerance = 1;   // tolerance on how much of a difference can each pair of assembled contigs (actual vs expected) length could be
    private static final int editDistanceTolerance = 3; // tolerance of the summed edit distance between expected contigs and corresponding actual assembled contigs

    // set up path and data
    @BeforeClass
    private static void setup(){
        String sgaPathString = System.getProperty("path.to.sga");
        if(null==sgaPathString){
            sgaPathString = System.getenv("path.to.sga");
        }
        sgaPath =  (null==sgaPathString) ? null : Paths.get(sgaPathString);

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

    @BeforeMethod
    public static void checkPATHTOSGAAvailability(){

        if(null==sgaPath){
            throw new SkipException("Skipping test because \"-Dpath.to.sga\" is set neither in system property nor as an environment variable.");
        }
    }

    @Test(groups = "sv")
    public void assemblyOneStepTest() throws IOException, InterruptedException, RuntimeException{

        final FileSystem fs = FileSystem.get( new Configuration() );
        final File outDir = Files.createTempDirectory( "whatever").toAbsolutePath().toFile();
        outDir.deleteOnExit();

        for(final Tuple2<Long, URI> breakpoint : rawFASTQFiles){

            final String failureMsg = RunSGAViaProcessBuilderOnSpark.performAssembly(breakpoint, sgaPath.toAbsolutePath().toString(), true, fs, outDir.getAbsolutePath())._2();
            Assert.assertTrue(failureMsg.isEmpty());

            final File expectedAssembledContigFile = new File( expectedAssembledFASTAFiles.get(breakpoint._1()) );
            final File actualAssembledContigFile   = new File(outDir, expectedAssembledContigFile.getName());

            compareContigs(actualAssembledContigFile, expectedAssembledContigFile);
        }
    }

    @Test(groups = "sv")
    public void assemblyStepByStepTest() throws IOException, InterruptedException, RuntimeException{

        for(final Tuple2<Long, URI> breakpoint : rawFASTQFiles){

            //prep: make temp dir, copy file
            final LocalFileSystem lfs = FileSystem.getLocal(new Configuration());

            final File rawFASTQFile = RunSGAViaProcessBuilderOnSpark.makeTempDirAndCopyFASTQToLocal(lfs, breakpoint._2(), breakpoint._1());

            final File expectedAssembledContigFile = new File( expectedAssembledFASTAFiles.get(breakpoint._1()) );

            stepByStepTestWorker(rawFASTQFile, expectedAssembledContigFile);
        }
    }

    private static void stepByStepTestWorker(final File rawFASTQFile, final File expectedAssembledContigFile) throws IOException, InterruptedException, RuntimeException{

        final File workingDir = rawFASTQFile.getParentFile();
        final List<SGAModule.RuntimeInfo> runtimeInfo = new ArrayList<>();

        ArrayList<Integer> editDistancesBetweenSeq = new ArrayList<>();
        final Integer zero = 0;

        final ArrayList<String> indexerArgs = new ArrayList<>();
        indexerArgs.add("--algorithm"); indexerArgs.add("ropebwt");
        indexerArgs.add("--check");
        indexerArgs.add("");

        final String filenamePrefix = FilenameUtils.getBaseName(rawFASTQFile.getName());

        final File actualPreppedFile = new File(workingDir, RunSGAViaProcessBuilderOnSpark.runSGAPreprocess(sgaPath, rawFASTQFile, workingDir, indexer, indexerArgs, runtimeInfo));
        final File expectedPreppedFile = new File(TEST_DATA_DIR, filenamePrefix + ".pp.fa");
        final String preppedFileName = compareNamesAndComputeSeqEditDist(actualPreppedFile, expectedPreppedFile, true, editDistancesBetweenSeq);
        for(final Integer d : editDistancesBetweenSeq){ Assert.assertEquals(d, zero); }

        editDistancesBetweenSeq = new ArrayList<>();
        final File preppedFile = new File(workingDir, preppedFileName);
        final File actualCorrectedFile = new File(workingDir, RunSGAViaProcessBuilderOnSpark.runSGACorrect(sgaPath, preppedFile, workingDir, indexer, indexerArgs, runtimeInfo));
        final File expectedCorrectedFile = new File(TEST_DATA_DIR, filenamePrefix + ".pp.ec.fa");
        final String correctedFileName = compareNamesAndComputeSeqEditDist(actualCorrectedFile, expectedCorrectedFile, true, editDistancesBetweenSeq);
        for(final Integer d : editDistancesBetweenSeq){ Assert.assertEquals(d, zero); }

        editDistancesBetweenSeq = new ArrayList<>();
        final File correctedFile = new File(workingDir, correctedFileName);
        final File actualFilterPassingFile = new File(workingDir, RunSGAViaProcessBuilderOnSpark.runSGAFilter(sgaPath, correctedFile, workingDir, runtimeInfo));
        final File expectedFilterPassingFile = new File(TEST_DATA_DIR, filenamePrefix + ".pp.ec.filter.pass.fa");
        final String filterPassingFileName = compareNamesAndComputeSeqEditDist(actualFilterPassingFile, expectedFilterPassingFile, true, editDistancesBetweenSeq);
        for(final Integer d : editDistancesBetweenSeq){ Assert.assertEquals(d, zero); }

        editDistancesBetweenSeq = new ArrayList<>();
        final File filterPassingFile = new File(workingDir, filterPassingFileName);
        final File actualRmdupFile = new File(workingDir, RunSGAViaProcessBuilderOnSpark.runSGARmDuplicate(sgaPath, filterPassingFile, workingDir, indexer, indexerArgs, runtimeInfo));
        final File expectedRmdupFile = new File(TEST_DATA_DIR, filenamePrefix + ".pp.ec.filter.pass.rmdup.fa");
        final String duplicateRemovedFileName = compareNamesAndComputeSeqEditDist(actualRmdupFile, expectedRmdupFile, false, editDistancesBetweenSeq);
        for(final Integer d : editDistancesBetweenSeq){ Assert.assertEquals(d, zero); }

        editDistancesBetweenSeq = new ArrayList<>();
        final File duplicateRemovedFile = new File(workingDir, duplicateRemovedFileName);
        final File actualMergedFile = new File(workingDir, RunSGAViaProcessBuilderOnSpark.runSGAFMMerge(sgaPath, duplicateRemovedFile, workingDir, indexer, indexerArgs, runtimeInfo));
        final File expectedMergedFile = new File(TEST_DATA_DIR, filenamePrefix + ".pp.ec.filter.pass.rmdup.merged.fa");
        final String mergedFileName = compareNamesAndComputeSeqEditDist(actualMergedFile, expectedMergedFile, false, editDistancesBetweenSeq);
        for(final Integer d : editDistancesBetweenSeq){ Assert.assertEquals(d, zero); }

        // final assembled contig test
        final File mergedFile = new File(workingDir, mergedFileName);
        final File actualAssembledContigsFile = new File(workingDir, RunSGAViaProcessBuilderOnSpark.runSGAOverlapAndAssemble(sgaPath, mergedFile, workingDir, runtimeInfo));

        compareContigs(actualAssembledContigsFile, expectedAssembledContigFile);
    }

    private static void compareContigs(final File actualAssembledContigFile,
                                       final File expectedAssembledContigFile)
            throws IOException, InterruptedException{

        final List<String> actualFASTAContents   = (null==actualAssembledContigFile  ) ? null : Files.readAllLines(Paths.get(actualAssembledContigFile.getAbsolutePath()  ));
        final List<String> expectedFASTAContents = (null==expectedAssembledContigFile) ? null : Files.readAllLines(Paths.get(expectedAssembledContigFile.getAbsolutePath()));

        // first create mapping from read length to sequence
        // (bad, but contig names are not guaranteed to be reproducible in different runs)
        final SortedMap<Integer, String> actualMap = collectContigsByLength(actualFASTAContents);

        final SortedMap<Integer, String> expectedMap = collectContigsByLength(expectedFASTAContents);

        // essentially, make sure the number of assembled contigs with unique length values are the same
        final SortedSet<Integer> actualLengthVals = new TreeSet<>(actualMap.keySet());
        final SortedSet<Integer> expectedLengthVals = new TreeSet<>(expectedMap.keySet());
        Assert.assertEquals(actualLengthVals.size(), expectedLengthVals.size());

        // first compare contig length values, with some tolerance
        final Iterator<Integer> itActual = actualLengthVals.iterator();
        final Iterator<Integer> itExpected = expectedLengthVals.iterator();
        // if the two sets are not exactly the same but difference is within tolerance, create new map
        final List<Tuple2<String, String>> actualVSexpectedPairs = new ArrayList<>();
        while(itActual.hasNext()){
            final Integer actualLength = itActual.next();
            final Integer expectedLength = itExpected.next(); // safe operation as the previous assertion guarantees two sets are of same size
            Assert.assertTrue( Math.abs(actualLength - expectedLength)<=lengthDiffTolerance );
            actualVSexpectedPairs.add(new Tuple2<>(actualMap.get(actualLength), expectedMap.get(expectedLength)));
        }

        // then compare sequences, aware of RC
        for(final Tuple2<String, String> pairOfSimilarLength: actualVSexpectedPairs){

            final String actualString = pairOfSimilarLength._1();
            final String expectedString = pairOfSimilarLength._2();

            final String rcOfActualString = new String(BaseUtils.simpleReverseComplement(actualString.getBytes(StandardCharsets.UTF_8)));

            final int dist = StringUtils.getLevenshteinDistance(actualString, expectedString);
            final int rcDist = StringUtils.getLevenshteinDistance(rcOfActualString, expectedString);

            boolean sequencesAreTheSame = false;
            boolean sequencesAreCloseEnough = false;

            final int minDist = (dist < rcDist ? dist : rcDist);

            if(actualString.equals(expectedString) || rcOfActualString.equals(expectedString)){
                sequencesAreCloseEnough = sequencesAreTheSame = true;
            }else{
                // two things: the unit test tests if sequences are similar enough to each other
                // but if the minimum edit distance is different, we want to see which one it is.
                sequencesAreCloseEnough = (minDist <= editDistanceTolerance);
                if(0!=minDist){
                    System.err.println("Contig that has nonzero edit distance is of length " + Integer.toString(actualString.length()) +
                                        "\nEdit distance:" + String.valueOf(minDist) +
                                        "\nactual sequence: " + actualString +
                                        "\nreverse complement of actual sequence: " + rcOfActualString +
                                        "\nexpected sequence: " + expectedString);
                }
            }

            Assert.assertTrue(sequencesAreTheSame || sequencesAreCloseEnough,
                              "Culprit contigs (expected, actual) of length: " + String.valueOf(expectedString.length()) + ", " + String.valueOf(actualString.length()) +
                              "With edit distance " + String.valueOf(minDist));
        }
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

    // utility function: for extracting read names and sequences from a fastq/fasta file
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
    private static SortedMap<Integer, String> collectContigsByLength(final List<String> contigFileContents){

        final Iterator<String> it = contigFileContents.iterator();

        final SortedMap<Integer, String> result = new TreeMap<>();
        int i=0;
        while(it.hasNext()){
            final String line = it.next();
            if((++i)%2==1) {
                continue;
            } else{
                result.put(line.length(), line);
            }
        }
        return result;
    }
}