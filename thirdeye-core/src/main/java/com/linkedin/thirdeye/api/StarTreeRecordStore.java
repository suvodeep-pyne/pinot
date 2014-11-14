package com.linkedin.thirdeye.api;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public interface StarTreeRecordStore extends Iterable<StarTreeRecord>
{
  /**
   * If a record exists in the store with the same dimension values, merges it; otherwise, adds it.
   *
   * @param record
   *  The record to be added or merged in the store
   */
  void update(StarTreeRecord record);

  @Override
  Iterator<StarTreeRecord> iterator();

  /**
   * Removes all records from this store
   */
  void clear();

  /**
   * Loads this store from persistent storage, or opens resources.
   *
   * @throws java.io.IOException
   *  If the store couldn't be loaded
   */
  void open() throws IOException;

  /**
   * Saves this store to persistent storage, or closes resources.
   *
   * @throws java.io.IOException
   */
  void close() throws IOException;

  /** @return the number of records in this record store */
  int getRecordCount();

  /** @return the number of bytes currently used by this record store */
  long getByteCount();

  /** @return the cardinality of a given dimension */
  int getCardinality(String dimensionName);

  /** @return the dimension with maximum cardinality */
  String getMaxCardinalityDimension();

  /** @return the dimension with maximum cardinality that's not in blacklist */
  String getMaxCardinalityDimension(Collection<String> blacklist);

  /** @return the set of dimension values seen for a named dimension in this store */
  Set<String> getDimensionValues(String dimensionName);

  /** @return the aggregates corresponding to the query */
  int[] getMetricSums(StarTreeQuery query);

  /** @return the size in bytes of an entry in this record store */
  int getEntrySize();

  /** @return a serialized representation of this record store */
  byte[] encode();

  /** @return The timestamp of the earliest record(s) in the store */
  Long getMinTime();

  /** @return The timestamp of the latest record(s) in the store */
  Long getMaxTime();

  /** Performs compaction on underlying store if possible to do so */
  void compact();
}
