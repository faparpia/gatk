package org.broadinstitute.hellbender.tools.spark.utils;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.annotations.VisibleForTesting;

import java.util.*;

/**
 * Hash set implementation that provides low memory overhead with a high load factor by using the hopscotch algorithm.
 * This is usually a little slower than the JDK's HashSet (neglecting GC time), but it uses much less memory.
 * It's probably a nice choice for very large collections.
 * You can make it pretty much equally fast as HashSet by replacing the prime-number table sizing with power-of-2 table
 * sizing, but then it'll behave just as badly as HashSet given crappy hashCode implementations.
 */
@DefaultSerializer(HopscotchHashSet.Serializer.class)
public final class HopscotchHashSet<T> extends AbstractSet<T> {
    // For power-of-2 table sizes add this line
    //private static final int maxCapacity = Integer.MAX_VALUE/2 + 1;

    private int capacity;
    // unused buckets contain null.  (this data structure does not support null entries.)
    // if the bucket is unused, the corresponding status byte is irrelevant, but is always set to 0.
    private int size;
    private T[] buckets;
    // format of the status bytes:
    // high bit set indicates that the bucket contains a "chain head" (i.e., an entry that naturally belongs in the
    // corresponding bucket).  high bit not set indicates a "squatter" (i.e., an entry that got placed here through the
    // collision resolution methodology).  we use Byte.MIN_VALUE (i.e., 0x80) to pick off this bit.
    // low 7 bits give the (unsigned) offset from the current entry to the next entry in the collision resolution chain.
    // if the low 7 bits are 0, then we'd be pointing at ourselves, which is nonsense, so that particular value marks
    // "end of chain" instead.  we use Byte.MAX_VALUE (i.e., 0x7f) to pick off these bits.
    private byte[] status;

    @VisibleForTesting static final double LOAD_FACTOR = .85;

    // largest prime numbers less than each half power of 2 from 2^8 to 2^31
    @VisibleForTesting static final int[] legalSizes = {
            251, 359, 509, 719, 1021, 1447, 2039, 2887, 4093, 5791, 8191, 11579, 16381, 23167, 32749, 46337, 65521,
            92681, 131071, 185363, 262139, 370723, 524287, 741431, 1048573, 1482907, 2097143, 2965819, 4194301, 5931641,
            8388593, 11863279, 16777213, 23726561, 33554393, 47453111, 67108859, 94906249, 134217689, 189812507,
            268435399, 379625047, 536870909, 759250111, 1073741789, 1518500213, 2147483647
    };
    private static final int SPREADER = 241;

    @SuppressWarnings("unchecked")
    public HopscotchHashSet( final int capacity ) {
        this.capacity = computeCapacity(capacity);
        this.size = 0;
        // Not unsafe, because the remainder of the API allows only elements known to be T's to be assigned to buckets.
        this.buckets = (T[])new Object[this.capacity];
        this.status = new byte[this.capacity];
    }

    public HopscotchHashSet( final Collection<T> collection ) {
        this(collection.size());
        addAll(collection);
    }

    public HopscotchHashSet( final Collection<T> collection, final float sizeInflator ) {
        this((int)(collection.size()*sizeInflator));
        addAll(collection);
    }

    @SuppressWarnings("unchecked")
    private HopscotchHashSet( final Kryo kryo, final Input input ) {
        final boolean oldReferencesState = kryo.getReferences();
        kryo.setReferences(false);
        capacity = input.readInt();
        size = 0;
        buckets = (T[])new Object[capacity];
        status = new byte[capacity];
        int size = input.readInt();
        while ( size-- > 0 ) {
            add((T)kryo.readClassAndObject(input));
        }
        kryo.setReferences(oldReferencesState);
    }

    private void serialize( final Kryo kryo, final Output output ) {
        final boolean oldReferencesState = kryo.getReferences();
        // this is actually screwy -- what we need to do is to turn off just the top-level reference resolution as
        // if we were using the ObjectOutputStream's writeUnshared method.
        // we're a set, after all, and each object will be unique.  but we don't know what's going on inside
        // the objects in the set -- they might well have shared references to other objects.
        // but with kryo there doesn't seem to be a way to do this -- you have to turn off all reference resolution.
        kryo.setReferences(false);
        output.writeInt(capacity);
        output.writeInt(size);
        int count = 0;
        for ( int idx = 0; idx != capacity; ++idx ) {
            if ( isChainHead(idx) ) {
                kryo.writeClassAndObject(output, buckets[idx]);
                count += 1;
            }
        }
        for ( int idx = 0; idx != capacity; ++idx ) {
            final T val = buckets[idx];
            if ( val != null && !isChainHead(idx) ) {
                kryo.writeClassAndObject(output, val);
                count += 1;
            }
        }
        kryo.setReferences(oldReferencesState);

        if ( count != size ) {
            throw new IllegalStateException("Failed to serialize the expected number of objects: expected="+size+" actual="+count+".");
        }
    }

    @Override
    public boolean contains( final Object value ) {
        if ( value == null ) return false;
        int bucketIndex = hashToIndex(value.hashCode());
        if ( !isChainHead(bucketIndex) ) return false;
        if ( buckets[bucketIndex].equals(value) ) return true;
        int offset;
        while ( (offset = getOffset(bucketIndex)) != 0 ) {
            bucketIndex = getIndex(bucketIndex, offset);
            if ( buckets[bucketIndex].equals(value) ) return true;
        }
        return false;
    }

    public int capacity() { return capacity; }

    @Override
    public int size() { return size; }

    @Override
    public Iterator<T> iterator() { return new CompleteIterator(); }

    /**
     * This special iterator type allows you to traverse individual hash buckets.
     * This lets you implement a multi-map on this collection by using a <K,V>-ish element type having a hashCode that
     * depends only on K.
     */
    public Iterator<T> bucketIterator( final int hashVal ) { return new BucketIterator(hashVal); }

    @Override
    public boolean add( final T entry ) {
        if ( entry == null ) throw new UnsupportedOperationException("This collection cannot contain null.");
        if ( size == capacity ) resize();
        try {
            return insert(entry) == entry;
        } catch ( final IllegalStateException ise ) {
            resize();
            return insert(entry) == entry;
        }
    }

    public T put( final T entry ) {
        if ( entry == null ) throw new UnsupportedOperationException("This collection cannot contain null.");
        if ( size == capacity ) resize();
        try {
            return insert(entry);
        } catch ( final IllegalStateException ise ) {
            resize();
            return insert(entry);
        }
    }

    @Override
    public boolean remove( final Object entry ) {
        if ( entry == null ) return false;
        int bucketIndex = hashToIndex(entry.hashCode());
        if ( buckets[bucketIndex] == null || !isChainHead(bucketIndex) ) return false;
        int predecessorIndex = -1;
        while ( true ) {
            final int offset = getOffset(bucketIndex);
            if ( buckets[bucketIndex].equals(entry) ) {
                if ( offset == 0 ) {
                    buckets[bucketIndex] = null;
                    status[bucketIndex] = 0;
                    if ( predecessorIndex != -1 ) status[predecessorIndex] -= getOffset(predecessorIndex);
                } else {
                    // move the item at the end of the chain into the hole we're creating by deleting this entry
                    int prevIndex = bucketIndex;
                    int nextIndex = getIndex(prevIndex, offset);
                    int offsetToNext;
                    while ( (offsetToNext = getOffset(nextIndex)) != 0 ) {
                        prevIndex = nextIndex;
                        nextIndex = getIndex(nextIndex, offsetToNext);
                    }
                    buckets[bucketIndex] = buckets[nextIndex];
                    buckets[nextIndex] = null;
                    status[prevIndex] -= getOffset(prevIndex);
                }
                size -= 1;
                return true;
            }
            if ( offset == 0 ) break;
            predecessorIndex = bucketIndex;
            bucketIndex = getIndex(bucketIndex, offset);
        }
        return false;
    }

    @Override
    public void clear() {
        for ( int idx = 0; idx != capacity; ++idx ) {
            buckets[idx] = null;
            status[idx] = 0;
        }
        size = 0;
    }

    private T insert( final T entry ) {
        final int bucketIndex = hashToIndex(entry.hashCode());

        // if there's a squatter where the new entry should go, move it elsewhere and put the entry there
        if ( buckets[bucketIndex] != null && !isChainHead(bucketIndex) ) evict(bucketIndex);

        // if the place where it should go is empty, just put the new entry there
        if ( buckets[bucketIndex] == null ) {
            buckets[bucketIndex] = entry;
            status[bucketIndex] = Byte.MIN_VALUE;
            size += 1;
            return entry;
        }

        // make sure the entry isn't already present
        int endOfChainIndex = bucketIndex;
        while ( true ) {
            // if entry is already in the set
            final T tableEntry = buckets[endOfChainIndex];
            if ( tableEntry.equals(entry) ) return tableEntry;
            final int offset = getOffset(endOfChainIndex);
            if ( offset == 0 ) break;
            endOfChainIndex = getIndex(endOfChainIndex, offset);
        }

        // find a place for the new entry
        final int emptyBucketIndex = insertIntoChain(bucketIndex, endOfChainIndex);

        // put the new entry into the empty bucket
        buckets[emptyBucketIndex] = entry;
        size += 1;
        return entry;
    }

    private int hashToIndex( final int hashVal ) {
        // For power-of-2 table sizes substitute this line
        // return (SPREADER*hashVal)&(capacity-1);
        return Math.floorMod(SPREADER*hashVal, capacity);
    }

    private int insertIntoChain( final int bucketIndex, final int endOfChainIndex ) {
        final int offsetToEndOfChain = getIndexDiff(bucketIndex, endOfChainIndex);

        // find an empty bucket for the new entry
        int emptyBucketIndex = findEmptyBucket(bucketIndex);

        // if the distance to the empty bucket is larger than this, we'll have to hopscotch
        final int maxOffset = offsetToEndOfChain + Byte.MAX_VALUE;

        // hopscotch the empty bucket into range if it's too far away
        int offsetToEmpty;
        while ( (offsetToEmpty = getIndexDiff(bucketIndex, emptyBucketIndex)) > maxOffset ) {
            emptyBucketIndex = hopscotch(bucketIndex, emptyBucketIndex);
        }

        // if the new entry lies downstream of the current chain end, just link it in
        if ( offsetToEmpty > offsetToEndOfChain ) {
            status[endOfChainIndex] += offsetToEmpty - offsetToEndOfChain;
        } else {
            linkIntoChain(bucketIndex, emptyBucketIndex);
        }

        return emptyBucketIndex;
    }

    // walk the chain until we find where the new slot gets linked in
    private void linkIntoChain( final int bucketIndex, final int emptyBucketIndex ) {
        int offsetToEmpty = getIndexDiff(bucketIndex, emptyBucketIndex);
        int tmpIndex = bucketIndex;
        int offset;
        while ( (offset = getOffset(tmpIndex)) < offsetToEmpty ) {
            tmpIndex = getIndex(tmpIndex, offset);
            offsetToEmpty -= offset;
        }
        offset -= offsetToEmpty;
        status[tmpIndex] -= offset;
        status[emptyBucketIndex] = (byte) offset;
    }

    private void evict( final int bucketToEvictIndex ) {
        final int bucketIndex = hashToIndex(buckets[bucketToEvictIndex].hashCode());
        final int offsetToEvictee = getIndexDiff(bucketIndex, bucketToEvictIndex);
        int emptyBucketIndex = findEmptyBucket(bucketIndex);
        int fromIndex = bucketIndex;
        while ( true ) {
            while ( getIndexDiff(bucketIndex, emptyBucketIndex) > offsetToEvictee ) {
                emptyBucketIndex = hopscotch(fromIndex, emptyBucketIndex);
            }
            if ( emptyBucketIndex == bucketToEvictIndex ) return;
            fromIndex = emptyBucketIndex;
            linkIntoChain(bucketIndex, emptyBucketIndex);
            int prevIndex = bucketIndex;
            int offsetToNext = getOffset(prevIndex);
            int nextIndex = getIndex(prevIndex, offsetToNext);
            while ( (offsetToNext = getOffset(nextIndex)) != 0 ) {
                prevIndex = nextIndex;
                nextIndex = getIndex(nextIndex, offsetToNext);
            }
            buckets[emptyBucketIndex] = buckets[nextIndex];
            buckets[nextIndex] = null;
            status[nextIndex] = 0;
            status[prevIndex] -= getOffset(prevIndex);
            emptyBucketIndex = nextIndex;
        }
    }

    private int findEmptyBucket( int bucketIndex ) {
        do {
            bucketIndex = getIndex(bucketIndex, 1);
        }
        while ( buckets[bucketIndex] != null );
        return bucketIndex;
    }

    private boolean isChainHead( final int bucketIndex ) {
        return (status[bucketIndex] & Byte.MIN_VALUE) != 0;
    }

    private int getOffset( final int bucketIndex ) {
        return status[bucketIndex] & Byte.MAX_VALUE;
    }

    private int getIndex( final int bucketIndex, final int offset ) {
        int result = bucketIndex + offset;
        if ( result >= capacity ) result -= capacity;
        else if ( result < 0 ) result += capacity;
        return result;
    }

    // bucket1 is assumed to be upstream of bucket2 (even if bucket2's index has wrapped)
    // i.e., the result is always positive
    private int getIndexDiff( final int bucketIndex1, final int bucketIndex2 ) {
        int result = bucketIndex2 - bucketIndex1;
        if ( result < 0 ) result += capacity;
        return result;
    }

    private int hopscotch( final int fromIndex, final int emptyBucketIndex ) {
        final int fromToEmptyDistance = getIndexDiff(fromIndex, emptyBucketIndex);
        int offsetToEmpty = Byte.MAX_VALUE;
        while ( offsetToEmpty > 1 ) {
            final int bucketIndex = getIndex(emptyBucketIndex, -offsetToEmpty);
            final int offsetInBucket = getOffset(bucketIndex);
            if ( offsetInBucket != 0 &&
                    offsetInBucket < offsetToEmpty &&
                    offsetToEmpty-offsetInBucket < fromToEmptyDistance ) {
                final int bucketToMoveIndex = getIndex(bucketIndex, offsetInBucket);
                move(bucketIndex, bucketToMoveIndex, emptyBucketIndex);
                return bucketToMoveIndex;
            }
            offsetToEmpty -= 1;
        }
        throw new IllegalStateException("Hopscotching failed at load factor "+(1.*size/capacity));
    }

    private void move( int predecessorBucketIndex, final int bucketToMoveIndex, final int emptyBucketIndex ) {
        int toEmptyDistance = getIndexDiff(bucketToMoveIndex, emptyBucketIndex);
        int nextOffset = getOffset(bucketToMoveIndex);
        if ( nextOffset == 0 || nextOffset > toEmptyDistance ) {
            status[predecessorBucketIndex] += toEmptyDistance;
        } else {
            status[predecessorBucketIndex] += nextOffset;
            toEmptyDistance -= nextOffset;
            predecessorBucketIndex = getIndex(bucketToMoveIndex, nextOffset);
            while ( (nextOffset = getOffset(predecessorBucketIndex)) != 0 && nextOffset < toEmptyDistance ) {
                toEmptyDistance -= nextOffset;
                predecessorBucketIndex = getIndex(predecessorBucketIndex, nextOffset);
            }
            status[predecessorBucketIndex] = (byte) toEmptyDistance;
        }
        if ( nextOffset != 0 ) {
            status[emptyBucketIndex] = (byte) (nextOffset - toEmptyDistance);
        }
        buckets[emptyBucketIndex] = buckets[bucketToMoveIndex];
        buckets[bucketToMoveIndex] = null;
        status[bucketToMoveIndex] = 0;
    }

    @SuppressWarnings("unchecked")
    private void resize() {
        if ( buckets == null ) throw new IllegalStateException("OMG I have no buckets.");
        final int oldCapacity = capacity;
        final int oldSize = size;
        final T[] oldBuckets = buckets;
        final byte[] oldStatus = status;

        capacity = bumpCapacity(capacity);
        size = 0;
        buckets = (T[])new Object[capacity];
        status = new byte[capacity];

        try {
            int idx = 0;
            do {
                final T entry = oldBuckets[idx];
                if ( entry != null ) insert(entry);
            }
            while ( (idx = (idx+127)%oldCapacity) != 0 );
        } catch ( IllegalStateException ise ) {
            capacity = oldCapacity;
            size = oldSize;
            buckets = oldBuckets;
            status = oldStatus;
            throw new IllegalStateException("Hopscotching failed at load factor "+1.*size/capacity+", and resizing didn't help.");
        }

        if ( size != oldSize ) {
            throw new IllegalStateException("Lost some elements during resizing.");
        }
    }

    private static int computeCapacity( final int size ) {
        // For power-of-2 table sizes substitute these lines
        /*
        if ( size > maxCapacity ) throw new IllegalArgumentException("Table can't be that big.");
        size = (int)(size/LOAD_FACTOR) - 1;
        size |= size >>> 1;
        size |= size >>> 2;
        size |= size >>> 4;
        size |= size >>> 8;
        size |= size >>> 16;
        return (size < 256) ? 256 : (size >= maxCapacity) ? maxCapacity : size + 1;
        */
        if ( size < LOAD_FACTOR*Integer.MAX_VALUE ) {
            final int augmentedSize = (int) (size / LOAD_FACTOR);
            for ( final int legalSize : legalSizes ) {
                if ( legalSize >= augmentedSize ) return legalSize;
            }
        }
        return legalSizes[legalSizes.length-1];
    }

    private static int bumpCapacity( final int capacity ) {
        // For power-of-2 table sizes substitute these lines
        /*
        if ( capacity == maxCapacity ) throw new IllegalStateException("Unable to increase capacity.");
        return capacity * 2;
        */
        if ( capacity <= legalSizes[legalSizes.length-2] ) {
            for ( int idx = 0; idx != legalSizes.length; ++idx ) {
                if ( legalSizes[idx] >= capacity ) return legalSizes[idx + 1];
            }
        }
        throw new IllegalStateException("Unable to increase capacity.");
    }

    private final class BucketIterator implements Iterator<T> {
        private int bucketIndex;

        BucketIterator( final int hashVal ) {
            final int bucketIndex = hashToIndex(hashVal);
            this.bucketIndex = isChainHead(bucketIndex) ? bucketIndex : buckets.length;
        }

        @Override public boolean hasNext() { return bucketIndex != buckets.length; }

        @Override public T next() {
            if ( bucketIndex == buckets.length ) throw new NoSuchElementException("HopscotchHashSet iterator is exhausted.");
            final T result = buckets[bucketIndex];
            if ( result == null ) throw new IllegalStateException("Unexpected null value during iteration.");
            final int offset = getOffset(bucketIndex);
            bucketIndex = offset != 0 ? getIndex(bucketIndex, offset) : buckets.length;
            return result;
        }
    }

    private final class CompleteIterator implements Iterator<T> {
        // Class Invariants:
        //  bucketHeadIndex is a valid bucket head until the iteration is complete.
        //    When iteration is complete it has the value buckets.length.
        //  currentElementIndex points to the element that the next method will return.
        //    It is always primed and ready to go until iteration is complete.
        //    When iteration is complete its value should be ignored (it's not set to anything in particular).
        //  previousElementIndex is set by calling next.  It is set to the invalid value -1 following a call to remove,
        //    as well as at the beginning (before the first call to next) of iteration.  It remains valid when iteration
        //    is complete, unless and until remove is called.
        private int bucketHeadIndex;
        private int currentElementIndex;
        private int previousElementIndex;

        CompleteIterator() { bucketHeadIndex = previousElementIndex = -1; nextBucketHead(); }

        @Override public boolean hasNext() { return bucketHeadIndex < buckets.length; }

        @Override public T next() {
            if ( !hasNext() ) throw new NoSuchElementException("Iterator exhausted.");

            previousElementIndex = currentElementIndex;

            final int offset = getOffset(currentElementIndex);
            // if we're at the end of a chain, advance to the next bucket, otherwise step to the next item in the chain.
            if ( offset == 0 ) nextBucketHead();
            else currentElementIndex = getIndex(currentElementIndex, offset);

            return buckets[previousElementIndex];
        }

        @Override public void remove() {
            if ( previousElementIndex < 0 ) throw new IllegalStateException("Remove without next.");

            HopscotchHashSet.this.remove(buckets[previousElementIndex]);

            // If we haven't deleted the end of a chain, we'll now have an unseen element under previousElementIndex.
            // So we need to back up and let the user know about it at the next call to the next method.  If we
            // have deleted an end of chain, then the bucket will be empty and no adjustment needs to be made.
            if ( buckets[previousElementIndex] != null ) currentElementIndex = previousElementIndex;

            // Set state to "invalid to call remove again".
            previousElementIndex = -1;
        }

        private void nextBucketHead() {
            while ( ++bucketHeadIndex < buckets.length ) {
                if ( isChainHead(bucketHeadIndex) ) {
                    currentElementIndex = bucketHeadIndex;
                    return;
                }
            }
        }
    }

    public static final class Serializer<T> extends com.esotericsoftware.kryo.Serializer<HopscotchHashSet<T>> {
        @Override
        public void write( final Kryo kryo, final Output output, final HopscotchHashSet<T> hopscotchHashSet ) {
            hopscotchHashSet.serialize(kryo, output);
        }

        @Override
        public HopscotchHashSet<T> read( final Kryo kryo, final Input input, final Class<HopscotchHashSet<T>> klass ) {
            return new HopscotchHashSet<>(kryo, input);
        }
    }
}
