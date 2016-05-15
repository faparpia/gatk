package org.broadinstitute.hellbender.tools.spark.sv;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.programgroups.StructuralVariationSparkProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import scala.Tuple2;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

// TODO: choose which parameters allowed to be tunable
// TODO: choose output contents (currently output information is more developer friendly than user friendly)
@CommandLineProgramProperties(
        summary        = "Program to call SGA to perform local assembly and return assembled contigs if successful, " +
                          "or runtime error messages if the process erred for some breakpoints.",
        oneLineSummary = "Perform SGA-based local assembly on fasta files on Spark.",
        programGroup   = StructuralVariationSparkProgramGroup.class)
public final class RunSGAViaProcessBuilderOnSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;

    @Argument(doc       = "Absolute path to SGA installation.",
              shortName = "sgaPath",
              fullName  = "fullPathToSGA",
              optional  = false)
    public String pathToSGA = null;

    @Argument(doc       = "An URI to header-less file where each line contains a breakpoint ID and " +
                            "the absolute path (an URI) to one raw FASTQ file delimited by a tab",
              shortName = "fl",
              fullName  = "fastqList",
              optional  = false)
    public String pathToFASTQListFile = null;

    @Argument(doc       = "An absolute path to a directory to write results to.",
              shortName = "outDir",
              fullName  = "outputDirectory",
              optional  = false)
    public String outputDir = null;

    @Argument(doc       = "To run k-mer based read correction, filter and duplication removal in SGA or not, with default parameters.",
              shortName = "correct",
              fullName  = "correctNFilter",
              optional  = true)
    public boolean runCorrectionSteps = false;

    @Override
    public boolean requiresReads(){
        return false;
    }

    @Override
    public void runTool(final JavaSparkContext ctx){

        final Path sgaPath = Paths.get(pathToSGA);

        // first load RDD of pair that has breakpoint ID as its first and URI to FASTQ file as its second
        final JavaRDD<String> rawFASTQFiles = ctx.textFile(Paths.get(pathToFASTQListFile).toAbsolutePath().toString());
        final JavaPairRDD<Long, URI> seqsArrangedByBreakpoints = rawFASTQFiles.mapToPair(RunSGAViaProcessBuilderOnSpark::assignFASTQToBreakpoints);
        seqsArrangedByBreakpoints.filter(pair -> (pair._2()!=null));

        // distribute/copy FASTQ file to local disks and perform assembly (temp files live in temp dir that cleans self up automatically)
        // then copy the resulted FASTA contig file back to the designated outputDir
        FileSystem proxy = null;
        try{
            proxy = FileSystem.get( ctx.hadoopConfiguration() );
        }catch (final IOException ex){
            throw new UserException("Cannot get configuration."); // Any better exception?
        }
        final FileSystem fs = proxy;
        final JavaPairRDD<Long, String> assembly = seqsArrangedByBreakpoints.mapToPair(entry -> performAssembly(entry, sgaPath, runCorrectionSteps, fs, outputDir));

        final int numOfErrMsgFiles = 10;
        assembly.filter(entry -> !entry._2().isEmpty())
                .repartition(numOfErrMsgFiles)
                .saveAsTextFile(outputDir);
    }

    /**
     * Converts from a line of text, delimited with a tab, where the first entry is the breakpoint ID and the second entry
     *   is URI to its associated FASTQ file on HDFS.
     * @param recordLine    a line of text containing the breakpoint ID and URI to associated FASTQ file
     * @return              a pair constructed by splitting the string into the ID part and the URI part
     * @throws UserException if the URI part of the string could not be successfully parsed
     */
    @VisibleForTesting
    static Tuple2<Long, URI> assignFASTQToBreakpoints(final String recordLine) throws UserException{
        final String[] recordForOneBreakPoint = recordLine.split("\t");
        final Long breakpointID = Long.valueOf(recordForOneBreakPoint[0]);
        try{
            final URI fastqURI = new URI(recordForOneBreakPoint[1]);
            return new Tuple2<>(breakpointID, fastqURI);
        }catch (final URISyntaxException e){
            throw new UserException("Could not parse URI for breakpoint: " + breakpointID.toString() + "\n" + e.getMessage());
        }
    }

    /**
     * Performs assembly on the FASTA files pointed to by the URI that is associated with the breakpoint identified by the long ID.
     * Actual assembly work is delegated to other functions.
     * @param fastqOfABreakpoint    the breakpoint ID and URI to the FASTQ file
     * @param sgaPath               full path to SGA
     * @param runCorrections        user's decision to run SGA's corrections (with default parameter values) or not
     * @param fs                    file system
     * @return                      failure message (if process erred) or empty string (if process succeeded) associated with the breakpoint ID
     * @throws IOException          if fails to create temporary directory on local filesystem or fails to copy FASTQ/FASTA file from/to fs
     */
    @VisibleForTesting
    static Tuple2<Long, String> performAssembly(final Tuple2<Long, URI> fastqOfABreakpoint,
                                                final Path sgaPath,
                                                final boolean runCorrections,
                                                final FileSystem fs,
                                                final String absPathToOutputDir)
    throws IOException{

        final Long breakpointID = fastqOfABreakpoint._1();
        final URI  uriToFASTQ   = fastqOfABreakpoint._2();

        final LocalFileSystem lfs = FileSystem.getLocal(new Configuration());
        final File localFASTQFile = makeTempDirAndCopyFASTQToLocal(lfs, uriToFASTQ, breakpointID);

        final SGAAssemblyResult assembledContigsFileAndRuntimeInfo = runSGAModulesInSerial(sgaPath, localFASTQFile, runCorrections);

        if(null!=assembledContigsFileAndRuntimeInfo.contigFileName) {
            fs.copyFromLocalFile(new org.apache.hadoop.fs.Path(localFASTQFile.getParentFile().getAbsolutePath(), assembledContigsFileAndRuntimeInfo.contigFileName),
                                 new org.apache.hadoop.fs.Path(absPathToOutputDir));
            return new Tuple2<>(breakpointID, "");
        }else{
            return new Tuple2<>(breakpointID, assembledContigsFileAndRuntimeInfo.getRuntimeInfoAsString());
        }
    }

    /**
     * Copy from hdfs to temp local dir
     * @param lfs            local filesystem
     * @param uriToFASTQ    uri to source
     * @param breakpointID  breakpoint ID
     * @return              Path to the copied FASTQ file living in the temp local dir
     * @throws IOException  if fails to either create the temporary directory or copy the FASTQ file
     */
    @VisibleForTesting
    static File makeTempDirAndCopyFASTQToLocal(final LocalFileSystem lfs, final URI uriToFASTQ, final Long breakpointID) throws IOException{

        final File workingDir = Files.createTempDirectory( "assembly" + breakpointID.toString() ).toAbsolutePath().toFile();
        workingDir.deleteOnExit();

        final String rawFASTQFileName = FilenameUtils.getName(uriToFASTQ.getPath());
        final org.apache.hadoop.fs.Path from = new org.apache.hadoop.fs.Path(uriToFASTQ);
        final org.apache.hadoop.fs.Path to   = new org.apache.hadoop.fs.Path(workingDir.toPath().toAbsolutePath().toString()+"/"+rawFASTQFileName);
        try{
            lfs.copyToLocalFile(from, to);
        }catch(final IOException ex){ // make sure the temp dir is deleted in case of exception
            workingDir.delete();
            throw new IOException(ex);
        }
        final File localFile = lfs.pathToFile(to);
        return localFile;
    }

    /**
     * Linear pipeline for running the SGA local assembly process on a particular FASTQ file for its associated putative breakpoint.
     *
     * @param sgaPath           full path on the executors to the SGA program
     * @param rawFASTQFile      FASTQ file in temp local working dir
     * @param runCorrections    to run SGA correction steps--correct, filter, rmdup, merge--or not
     * @return                  the result accumulated through running the pipeline, where the contigs could be null if the process erred.
     */
    @VisibleForTesting
    static SGAAssemblyResult runSGAModulesInSerial(final Path sgaPath,
                                                   final File rawFASTQFile,
                                                   final boolean runCorrections){

        final File tempWorkingDir = rawFASTQFile.getParentFile();

        // the index module is used frequently, so make single instance and pass around
        final SGAModule indexer = new SGAModule("index");
        final List<String> indexerArgs = new ArrayList<>();
        indexerArgs.add("--algorithm"); indexerArgs.add("ropebwt");
        indexerArgs.add("--check");
        indexerArgs.add("");

        // collect runtime information along the way
        final List<SGAModule.RuntimeInfo> runtimeInfo = new ArrayList<>();

        String preppedFileName = runAndStopEarly("preprocess", rawFASTQFile, sgaPath, tempWorkingDir, indexer, indexerArgs, runtimeInfo);
        if( null == preppedFileName ){
            return new SGAAssemblyResult(null, runtimeInfo);
        }

        if(runCorrections){// correction, filter, and remove duplicates stringed together
            final File preprocessedFile = new File(tempWorkingDir, preppedFileName);

            preppedFileName = runAndStopEarly("correct", preprocessedFile, sgaPath, tempWorkingDir, indexer, indexerArgs, runtimeInfo);
            if( null == preppedFileName ){
                return new SGAAssemblyResult(null, runtimeInfo);
            }
            final File correctedFile = new File(tempWorkingDir, preppedFileName);

            preppedFileName = runAndStopEarly("filter", correctedFile, sgaPath, tempWorkingDir, indexer, indexerArgs, runtimeInfo);
            if( null == preppedFileName ){
                return new SGAAssemblyResult(null, runtimeInfo);
            }
            final File filterPassingFile = new File(tempWorkingDir, preppedFileName);

            preppedFileName = runAndStopEarly("rmdup", filterPassingFile, sgaPath, tempWorkingDir, indexer, indexerArgs, runtimeInfo);
            if( null == preppedFileName ){
                return new SGAAssemblyResult(null, runtimeInfo);
            }
        }

        final File fileToMerge      = new File(tempWorkingDir, preppedFileName);
        final String fileNameToAssemble = runAndStopEarly("fm-merge", fileToMerge, sgaPath, tempWorkingDir, indexer, indexerArgs, runtimeInfo);
        if(null == fileNameToAssemble){
            return new SGAAssemblyResult(null, runtimeInfo);
        }

        final File fileToAssemble   = new File(tempWorkingDir, fileNameToAssemble);
        final String contigsFileName = runAndStopEarly("assemble", fileToAssemble, sgaPath, tempWorkingDir, indexer, indexerArgs, runtimeInfo);
        if(null == contigsFileName){
            return new SGAAssemblyResult(null, runtimeInfo);
        }

        return new SGAAssemblyResult(contigsFileName, runtimeInfo);
    }

    /**
     * Call the right sga module, log runtime information, and return the output file name if succeed.
     * If process erred, the string returned is null.
     * @param moduleName            SGA module name to be run
     * @param inputFASTQFile        FASTQ file tobe fed to SGA module
     * @param sgaPath               full path to the SGA program
     * @param workingDir            directory the SGA pipeline is working in
     * @param indexer               module representing SGA index
     * @param indexerArgs           arguments used by SGA index
     * @param collectedRuntimeInfo  runtime information collected along the process
     * @return                      the name of file produced by running this SGA module
     */
    private static String runAndStopEarly(final String moduleName,
                                          final File inputFASTQFile,
                                          final Path sgaPath,
                                          final File workingDir,
                                          final SGAModule indexer,
                                          final List<String> indexerArgs,
                                          final List<SGAModule.RuntimeInfo> collectedRuntimeInfo){

        String filenameToReturn = null;
        if(moduleName.equalsIgnoreCase("preprocess")){
            filenameToReturn = runSGAPreprocess(sgaPath, inputFASTQFile, workingDir, indexer, indexerArgs, collectedRuntimeInfo);
        }else if(moduleName.equalsIgnoreCase("correct")){
            filenameToReturn = runSGACorrect(sgaPath, inputFASTQFile, workingDir, indexer, indexerArgs, collectedRuntimeInfo);
        }else if(moduleName.equalsIgnoreCase("filter")){
            filenameToReturn = runSGAFilter(sgaPath, inputFASTQFile, workingDir, collectedRuntimeInfo);
        }else if(moduleName.equalsIgnoreCase("rmdup")){
            filenameToReturn = runSGARmDuplicate(sgaPath, inputFASTQFile, workingDir, indexer, indexerArgs, collectedRuntimeInfo);
        }else if(moduleName.equalsIgnoreCase("fm-merge")){
            filenameToReturn = runSGAFMMerge(sgaPath, inputFASTQFile, workingDir, indexer, indexerArgs, collectedRuntimeInfo);
        }else if(moduleName.equalsIgnoreCase("assemble")){
            filenameToReturn = runSGAOverlapAndAssemble(sgaPath, inputFASTQFile, workingDir, collectedRuntimeInfo);
        }else{
            throw new GATKException("Wrong module called"); // should never occur, implementation mistake
        }

        final SGAModule.RuntimeInfo.ReturnStatus returnStatus = collectedRuntimeInfo.get(collectedRuntimeInfo.size()-1).returnStatus;

        if(!(returnStatus.equals( SGAModule.RuntimeInfo.ReturnStatus.SUCCESS))){
            return null;
        }else{
            return filenameToReturn;
        }
    }

    @VisibleForTesting
    static String runSGAPreprocess(final Path sgaPath,
                                   final File inputFASTQFile,
                                   final File outputDirectory,
                                   final SGAModule indexer,
                                   final List<String> indexerArgs,
                                   final List<SGAModule.RuntimeInfo> collectedRuntimeInfo){

        final String prefix = FilenameUtils.getBaseName(inputFASTQFile.getName());
        final String preprocessedFASTAFileName = prefix+".pp.fa";

        final SGAModule preprocess = new SGAModule("preprocess");
        final List<String> ppArgs = new ArrayList<>();
        ppArgs.add("--pe-mode");    ppArgs.add("2");
        ppArgs.add("--pe-orphans"); ppArgs.add(prefix+".pp.orphan.fa");
        ppArgs.add("--out");        ppArgs.add(preprocessedFASTAFileName);
        ppArgs.add(inputFASTQFile.getName());

        final SGAModule.RuntimeInfo ppInfo = preprocess.run(sgaPath, outputDirectory, ppArgs);
        collectedRuntimeInfo.add(ppInfo);

        indexerArgs.set(indexerArgs.size()-1, preprocessedFASTAFileName);
        final SGAModule.RuntimeInfo indexerInfo = indexer.run(sgaPath, outputDirectory, indexerArgs);
        collectedRuntimeInfo.add(indexerInfo);

        return preprocessedFASTAFileName;
    }

    @VisibleForTesting
    static String runSGACorrect(final Path sgaPath,
                                final File inputFASTAFile,
                                final File outputDirectory,
                                final SGAModule indexer,
                                final List<String> indexerArgs,
                                final List<SGAModule.RuntimeInfo> collectedRuntimeInfo){
        return runSimpleModuleFollowedByIndexing(sgaPath, "correct", ".ec.fa", inputFASTAFile, outputDirectory, indexer, indexerArgs, collectedRuntimeInfo);
    }

    @VisibleForTesting
    static String runSGAFilter(final Path sgaPath,
                               final File inputFASTAFile,
                               final File outputDirectory,
                               final List<SGAModule.RuntimeInfo> collectedRuntimeInfo){

        final String prefix = FilenameUtils.getBaseName(inputFASTAFile.getName());
        final SGAModule filter = new SGAModule("filter");
        final List<String> filterArgs = new ArrayList<>();
        filterArgs.add(prefix+".fa");
        final SGAModule.RuntimeInfo filterInfo = filter.run(sgaPath, outputDirectory, filterArgs);
        collectedRuntimeInfo.add(filterInfo);

        return prefix+".filter.pass.fa";
    }

    @VisibleForTesting
    static String runSGARmDuplicate(final Path sgaPath,
                                    final File inputFASTAFile,
                                    final File outputDirectory,
                                    final SGAModule indexer,
                                    final List<String> indexerArgs,
                                    final List<SGAModule.RuntimeInfo> collectedRuntimeInfo){
        return runSimpleModuleFollowedByIndexing(sgaPath, "rmdup", ".rmdup.fa", inputFASTAFile, outputDirectory, indexer, indexerArgs, collectedRuntimeInfo);
    }

    @VisibleForTesting
    static String runSGAFMMerge(final Path sgaPath,
                                final File inputFASTAFile,
                                final File outputDirectory,
                                final SGAModule indexer,
                                final List<String> indexerArgs,
                                final List<SGAModule.RuntimeInfo> collectedRuntimeInfo){
        return runSimpleModuleFollowedByIndexing(sgaPath, "fm-merge", ".merged.fa", inputFASTAFile, outputDirectory, indexer, indexerArgs, collectedRuntimeInfo);
    }

    @VisibleForTesting
    static String runSGAOverlapAndAssemble(final Path sgaPath,
                                           final File inputFASTAFile,
                                           final File outputDirectory,
                                           final List<SGAModule.RuntimeInfo> collectedRuntimeInfo){

        final SGAModule overlap = new SGAModule("overlap");
        final List<String> overlapArgs = new ArrayList<>();
        overlapArgs.add(inputFASTAFile.getName());

        final SGAModule.RuntimeInfo overlapInfo = overlap.run(sgaPath, outputDirectory, overlapArgs);
        collectedRuntimeInfo.add(overlapInfo);

        final String prefix = FilenameUtils.getBaseName(inputFASTAFile.getName());

        final SGAModule assemble = new SGAModule("assemble");
        final List<String> assembleArgs = new ArrayList<>();
        assembleArgs.add("--out-prefix"); assembleArgs.add(prefix);
        assembleArgs.add(prefix+".asqg.gz");
        final SGAModule.RuntimeInfo assembleInfo = assemble.run(sgaPath, outputDirectory, assembleArgs);
        collectedRuntimeInfo.add(assembleInfo);

        return prefix+"-contigs.fa";
    }

    // boiler plate code for running simple sga modules (simple in the sense that no options needs to be specified to make it work)
    // that perform a task followed by indexing its output
    private static String runSimpleModuleFollowedByIndexing(final Path sgaPath,
                                                            final String moduleName,
                                                            final String extensionToAppend,
                                                            final File inputFASTAFile,
                                                            final File outputDirectory,
                                                            final SGAModule indexer,
                                                            final List<String> indexerArgs,
                                                            final List<SGAModule.RuntimeInfo> collectedRuntimeInfo){

        final SGAModule module = new SGAModule(moduleName);
        final List<String> args = new ArrayList<>();
        args.add(inputFASTAFile.getName());

        final SGAModule.RuntimeInfo moduleInfo = module.run(sgaPath, outputDirectory, args);
        collectedRuntimeInfo.add(moduleInfo);

        final String outputFileName = FilenameUtils.getBaseName(inputFASTAFile.getName()) + extensionToAppend;

        indexerArgs.set(indexerArgs.size()-1, outputFileName);
        final SGAModule.RuntimeInfo indexerInfo = indexer.run(sgaPath, outputDirectory, indexerArgs);
        collectedRuntimeInfo.add(indexerInfo);

        return outputFileName;
    }
    /**
     * Final return type of the whole process of SGA local assembly.
     * contigFileName is the name of the final FASTA file where the assembled contigs live, if the process executed successfully, or null if not.
     * runtimeInformation contains the runtime information logged along the process up until the process erred, if errors happen,
     *   or until the last step if no errors occur along the line.
     *   The list is organized along the process of executing the assembly pipeline.
     */
    @VisibleForTesting
    static final class SGAAssemblyResult implements Serializable{
        private static final long serialVersionUID = 1L;

        public final String contigFileName;
        public final List<SGAModule.RuntimeInfo> collectiveRuntimeInfo;

        public SGAAssemblyResult(final String contigFileName, final List<SGAModule.RuntimeInfo> collectedRuntimeInfo){
            this.contigFileName         = contigFileName;
            this.collectiveRuntimeInfo  = collectedRuntimeInfo;
        }

        /**
         * Returns the message accumulated.
         * @return
         */
        public String getRuntimeInfoAsString(){
            String msg = "";
            for(final SGAModule.RuntimeInfo info : collectiveRuntimeInfo){
                msg += info.toString();
            }
            return msg;
        }
    }
}