package org.broadinstitute.hellbender.tools.spark.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

/**
 * Unit tests for HopscotchHashSet.
 */
public class HopscotchHashSetTest extends BaseTest {
    private static final int RAND_SEED = 0xdeadf00;
    private static final int HHASH_NVALS = 10000;
    private static final int N_TRIALS = 1000;

    @Test(groups = "spark")
    void legalCapacitiesTest() {
        final int[] caps = HopscotchHashSet.legalSizes;
        final int nCaps = caps.length;
        // test that they're spaced properly -- each is supposed to be about sqrt(2) bigger than the previous one
        for ( int idx = 1; idx < nCaps; ++idx ) {
            final double err = Math.abs(1. - Math.sqrt(2.)*caps[idx-1]/caps[idx]);
            Assert.assertTrue(err < .015, "testing capacity "+caps[idx]+" at index "+idx);
        }
        // test that they're all primes
        for ( int idx = 0; idx < nCaps; ++idx ) {
            Assert.assertTrue(isPrime(caps[idx]), "testing capacity "+caps[idx]+" at index "+idx);
        }
    }

    private static boolean isPrime( final long iii ) {
        if ( iii % 2 == 0 || iii %3 == 0 ) return false;
        long iFact = 5;
        while ( iFact*iFact <= iii ) {
            if ( iii % iFact == 0 || iii % (iFact+2) == 0 ) return false;
            iFact += 6;
        }
        return true;
    }

    @Test(groups = "spark")
    void loadRandomIntsTest() {
        final Random rng = new Random(RAND_SEED);
        for ( int trialNo = 0; trialNo != N_TRIALS; ++trialNo ) {
            final HashSet<Integer> hashSet = new HashSet<>();
            final HopscotchHashSet<Integer> hHashSet = new HopscotchHashSet<>(HHASH_NVALS);
            final String trialMsg = "trialNo="+trialNo;
            for ( int valNo = 0; valNo != HHASH_NVALS; ++valNo ) {
                final Integer randVal = rng.nextInt();
                hHashSet.add(randVal);
                hashSet.add(randVal);
            }
            Assert.assertEquals(hashSet.size(), hHashSet.size(), trialMsg);
            for ( final Integer val : hashSet ) {
                Assert.assertTrue(hHashSet.contains(val), trialMsg+", testVal="+val);
            }
            for ( final Integer val : hHashSet ) {
                Assert.assertTrue(hashSet.contains(val), trialMsg+", testVal="+val);
            }

            for ( final Integer val : hashSet ) {
                Assert.assertTrue(hHashSet.remove(val));
            }
            hashSet.clear();

            Assert.assertEquals(hashSet.size(), hHashSet.size(), trialMsg);
            for ( final Integer val : hashSet ) {
                Assert.assertTrue(hHashSet.contains(val), trialMsg+", testVal="+val);
            }
            for ( final Integer val : hHashSet ) {
                Assert.assertTrue(hashSet.contains(val), trialMsg+", testVal="+val);
            }
        }
    }

    @Test(groups = "spark")
    void removeAllWithIteratorTest() {
        final Random rng = new Random(RAND_SEED);
        final HopscotchHashSet<Integer> hHashSet = new HopscotchHashSet<>(HHASH_NVALS);
        for ( int valNo = 0; valNo != HHASH_NVALS; ++valNo ) {
            hHashSet.add(rng.nextInt());
        }
        final Iterator<Integer> itr = hHashSet.iterator();
        while ( itr.hasNext() ) {
            final Integer next = itr.next();
            itr.remove();
            Assert.assertFalse(hHashSet.contains(next));
        }
        Assert.assertEquals(hHashSet.size(), 0);
    }

    @Test(groups = "spark")
    void serializationTest() {
        final Random rng = new Random(RAND_SEED);
        final HopscotchHashSet<Integer> hHashSet = new HopscotchHashSet<>(HHASH_NVALS);
        for ( int valNo = 0; valNo != HHASH_NVALS; ++valNo ) {
            hHashSet.add(rng.nextInt());
        }

        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final Output out = new Output(bos);
        final Kryo kryo = new Kryo();
        kryo.writeClassAndObject(out, hHashSet);
        out.flush();

        final ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        final Input in = new Input(bis);
        @SuppressWarnings("unchecked")
        final HopscotchHashSet<Integer> hHashSet2 = (HopscotchHashSet<Integer>)kryo.readClassAndObject(in);

        Assert.assertEquals(hHashSet.size(), hHashSet2.size());
        for ( final Integer val : hHashSet ) {
            Assert.assertTrue(hHashSet2.contains(val));
        }
        for ( final Integer val : hHashSet2 ) {
            Assert.assertTrue(hHashSet.contains(val));
        }
    }
}
