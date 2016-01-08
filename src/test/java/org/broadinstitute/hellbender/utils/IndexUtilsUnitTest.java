package org.broadinstitute.hellbender.utils;

import htsjdk.tribble.index.Index;
import htsjdk.tribble.index.IndexFactory;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;

public final class IndexUtilsUnitTest extends BaseTest{

    @DataProvider(name= "okFeatureFiles")
    public Object[][] okFeatureFiles() {
        return new Object[][] {
                { new File(getToolTestDataDir(), "test_variants_for_index.vcf")},
                { new File(getToolTestDataDir(), "test_variants_for_index.gvcf.vcf")},
                { new File(getToolTestDataDir(), "test_bed_for_index.bed")},
        };
    }

    @Test(dataProvider = "okFeatureFiles")
    public void testLoadIndex(final File featureFile) throws Exception {
        final Index index = IndexUtils.loadTribbleIndex(featureFile);
        Assert.assertNotNull(index);
    }

    @DataProvider(name= "okFeatureFilesTabix")
    public Object[][] okFeatureFilesTabix() {
        return new Object[][] {
                { new File(getToolTestDataDir(), "test_variants_for_index.vcf.bgz")},
        };
    }

    @Test(dataProvider = "okFeatureFilesTabix")
    public void testLoadTabixIndex(final File featureFile) throws Exception {
        final Index index = IndexUtils.loadTabixIndex(featureFile);
        Assert.assertNotNull(index);
    }

    @DataProvider(name= "failTabixIndexFiles")
    public Object[][] failTabixIndexFiles() {
        return new Object[][] {
                { new File(getToolTestDataDir(), "test_variants_for_index.vcf")},
                { new File(getToolTestDataDir(), "test_variants_for_index.gvcf.vcf")},
                { new File(getToolTestDataDir(), "test_bed_for_index.bed")},
        };
    }

    @Test(dataProvider = "failTabixIndexFiles")
    public void testFailLoadTabixIndex(final File featureFile) throws Exception {
        final Index index = IndexUtils.loadTabixIndex(featureFile);
        Assert.assertNull(index);
    }

    @DataProvider(name= "failTribbleIndexFiles")
    public Object[][] failTribbleIndexFiles() {
        return new Object[][] {
                { new File(getToolTestDataDir(), "test_variants_for_index.vcf.bgz")},
        };
    }

    @Test(dataProvider = "failTribbleIndexFiles")
    public void testFailLoadTribbleIndex(final File featureFile) throws Exception {
        final Index index = IndexUtils.loadTribbleIndex(featureFile);
        Assert.assertNull(index);
    }

    @Test
    public void testLoadIndexAcceptOldIndex() throws Exception {
        final File featureFile = new File(getToolTestDataDir(), "test_variants_for_index.newerThanIndex.vcf");
        final File featureFileIdx = new File(getToolTestDataDir(), "test_variants_for_index.newerThanIndex.vcf.idx");
        final File tmpDir = BaseTest.createTempDir("testLoadIndexAcceptOldIndex");

        Files.copy(featureFile.toPath(), tmpDir.toPath().resolve(featureFile.toPath().getFileName()));
        Files.copy(featureFileIdx.toPath(), tmpDir.toPath().resolve(featureFileIdx.toPath().getFileName()));
        featureFile.setLastModified(System.currentTimeMillis()); //touch the file but the index
        File tmpVcf= new File(tmpDir, featureFile.getName());
        final Index index = IndexUtils.loadTribbleIndex(tmpVcf);
        Assert.assertNotNull(index);
        //this should NOT blow up (files newer than indices are tolerated)
    }

    @Test
    public void testLoadIndex_noIndex() throws Exception {
        final File featureFile = new File(getToolTestDataDir(), "test_variants_for_index.noIndex.vcf");
        final Index index = IndexUtils.loadTribbleIndex(featureFile);
        Assert.assertNull(index);
    }

    @Test
    public void testCheckIndexModificationTime() throws Exception {
        final File vcf = new File(getToolTestDataDir(), "test_variants_for_index.vcf");
        final File vcfIdx = new File(getToolTestDataDir(), "test_variants_for_index.vcf.idx");
        final Index index = IndexFactory.loadIndex(vcfIdx.getAbsolutePath());
        IndexUtils.checkIndexModificationTime(vcf, vcfIdx, index);//no blowup
    }
}
