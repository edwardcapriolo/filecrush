/*
   Copyright 2011 m6d.com

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package com.m6d.filecrush.crush;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.fs.FileStatus;

/**
 * <p>
 * Arranges files into buckets. Callers must interact with this class in the following order:
 * </p>
 * <ol>
 * <li>Invoke {@link #reset(String)}.</li>
 * <li>Invoke {@link #add(FileStatus)} zero or more times.</li>
 * <li>Invoke {@link #createBuckets()}.</li>
 * <li>Go to 1 or throw away instance.</li>
 * </ol>
 *
 * <p>
 * The bucketing algorithm is:
 * </p>
 *
 * <ol>
 * <li>Calculate the number of buckets as floor(total bytes / block size). Add one if there is a remainder.</li>
 * <li>Sort the files in order of <b>descending</b> size.</li>
 * <li>Add each file to the bucket that has the least size.</li>
 * <li>Remove any buckets containing one file only</li>
 * </ol>
 */
class Bucketer {
	/**
	 * The maximum number of buckets to create.
	 */
	private final int maxBuckets;

	/**
	 * The size of the files to create. Used in the bucketing algorithm.
	 */
	private final long bucketSize;

	/**
	 * The items to consider for bucketing.
	 */
	private final List<HasSize> items = new LinkedList<HasSize>();

	/**
	 * The total number of bytes represented by the files in {@link #items}.
	 */
	private long size;

	/**
	 * The directory being bucketed.
	 */
	private String dir;

	/**
	 * Do not return buckets containing a single item from {@link #createBuckets()}.
	 */
	private final boolean excludeSingleItemBuckets;

	public Bucketer(int numBuckets, boolean excludeSingleItemBuckets) {
		this(numBuckets, 0, excludeSingleItemBuckets);
	}

	public Bucketer(int maxBuckets, long bucketSize, boolean excludeSingleItemBuckets) {
		super();

		if (1 > maxBuckets) {
			throw new IllegalArgumentException("Must have at least one bucket: " + maxBuckets);
		}

		this.maxBuckets = maxBuckets;

		if (0 > bucketSize) {
			throw new IllegalArgumentException("Bucket size must be zero or positive: " + bucketSize);
		}

		this.bucketSize = bucketSize;
		this.excludeSingleItemBuckets = excludeSingleItemBuckets;
	}

	/**
	 * Returns map from bucket to files that are in that bucket. Buckets are guaranteed to contain more than one file and will be
	 * approximately the same size in bytes (summing the sizes of all the files in that bucket). After this method returns,
	 * {@link #reset(String)} must be called before this instance can be called again.
	 */
	public List<Bucket> createBuckets() {
		if (null == dir) {
			throw new IllegalStateException("No directory set");
		}

		/*
		 * Sort the files in order of descending size.
		 */
		Collections.sort(items, DESCENDING_SIZE);

		LinkedList<Bucket> buckets = new LinkedList<Bucketer.Bucket>();

		for (long remaining = size; remaining > 0 && buckets.size() < maxBuckets; remaining -= bucketSize) {
			buckets.add(new Bucket(format("%s-%d", dir, buckets.size())));
		}

		int numBuckets = buckets.size();

		if (1 == numBuckets) {
			Bucket bucket = buckets.getFirst();

			for (HasSize file : items) {
				bucket.add(file);
			}
		} else {
			/*
			 * Add the files to the smallest bucket.
			 */
			for (HasSize item : items) {
				ListIterator<Bucket> iterator = buckets.listIterator();

				Bucket bucket = iterator.next();
				bucket.add(item);

				iterator.remove();

				/*
				 * Reposition the bucket in the list to preserve order by ascending bucket size.
				 */
				while (buckets.size() < numBuckets && iterator.hasNext()) {
					Bucket other = iterator.next();

					if (other.bytes > bucket.bytes) {
							iterator.previous();
							iterator.add(bucket);
					}
				}

				if (buckets.size() < numBuckets) {
					/*
					 * This bucket is now the biggest one.
					 */
					buckets.add(bucket);
				}
			}
		}

		if (excludeSingleItemBuckets) {
			for (Iterator<Bucket> iter = buckets.iterator(); iter.hasNext(); ) {
				Bucket bucket = iter.next();

				if (bucket.contents.size() < 2) {
					iter.remove();
				}
			}
		}

		/*
		 * Empty the state for the next invocation of reset.
		 */
		dir = null;
		items.clear();
		size = 0;

		return buckets;
	}

	/**
	 * Add an item for consideration. If the item has zero size, then it is ignored.
	 */
	public void add(HasSize item) {
		if (null == dir) {
			throw new IllegalStateException("No directory set");
		}

		long itemSize = item.size();

		if (0 != itemSize) {
			items.add(item);
			size += itemSize;
		}
	}

	/**
	 * Returns the count of items being considered.
	 */
	int count() {
		return items.size();
	}

	/**
	 * Returns the total size of all the items being considered.
	 */
	long size() {
		return size;
	}

	/**
	 * Resets the instance for the directory. The given name is used to name the buckets.
	 *
	 * @param dir
	 *          Directory name. Must not be null or empty.
	 */
	public void reset(String dir) {
		if (dir.equals("")) {
			throw new IllegalArgumentException("Directory is empty");
		}

		this.dir = dir;

		items.clear();
		size = 0;
	}

	String dir() {
		return dir;
	}

	public static class Bucket implements HasSize {

		private final List<String> contents;

		private final String name;

		private long bytes;

		public Bucket(String name) {
			super();

			this.name = name;
			this.contents = new LinkedList<String>();
		}

		public Bucket(String name, List<String> contents, long bytes) {
			super();

			this.contents = contents;
			this.name = name;
			this.bytes = bytes;
		}

		private void add(HasSize hasSize) {
			contents.add(hasSize.id());
			bytes += hasSize.size();
		}

		public List<String> contents() {
			return unmodifiableList(contents);
		}

		public String name() {
			return name;
		}

		public long bytes() {
			return bytes;
		}

		@Override
		public String id() {
			return name();
		}

		@Override
		public long size() {
			return bytes();
		}

		@Override
		public String toString() {
			return format("%s[%s, %d, %s]", getClass().getSimpleName(), name, bytes, contents);
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof Bucket)) {
				return false;
			}

			Bucket other = (Bucket) obj;

			return name.equals(other.name) && bytes == other.bytes && contents.equals(other.contents);
		}

		@Override
		public int hashCode() {
			return name.hashCode();
		}
	}

	private static final Comparator<HasSize> DESCENDING_SIZE = new Comparator<HasSize>() {
		@Override
		public int compare(HasSize o1, HasSize o2) {
			long l1 = o1.size();
			long l2 = o2.size();

			if (l1 < l2) {
				return 1;
			}

			if (l1 > l2) {
				return -1;
			}

			return 0;
		}
	};

	interface HasSize {
		String id();

		long size();
	}
}
