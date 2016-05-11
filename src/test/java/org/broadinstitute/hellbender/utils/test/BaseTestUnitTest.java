package org.broadinstitute.hellbender.utils.test;

import org.testng.Assert;
import org.testng.annotations.Test;

public class BaseTestUnitTest {

    @Test
    public void testNormalizeScientificNotation(){
        Assert.assertEquals(BaseTest.normalizeScientificNotation("-2.172e+00"), BaseTest.normalizeScientificNotation("-2.172"));
    }
}
