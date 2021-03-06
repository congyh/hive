/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.BasicTxnInfo;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * This cache keeps information in memory about the table modifications so materialized views
 * can verify their invalidation time, i.e., the moment after materialization on which the
 * first transaction to the tables they used happened. This information is kept in memory
 * to check the invalidation quickly. However, we store enough information in the metastore
 * to bring this cache up if the metastore is restarted or would crashed. This cache lives
 * in the metastore server.
 */
public final class MaterializationsInvalidationCache {

  private static final Logger LOG = LoggerFactory.getLogger(MaterializationsInvalidationCache.class);

  /* Singleton */
  private static final MaterializationsInvalidationCache SINGLETON = new MaterializationsInvalidationCache();

  /* If this boolean is true, this class has no functionality. Only for debugging purposes. */
  private boolean disable;

  /* Key is the database name. Each value is a map from the unique view qualified name to
   * the materialization invalidation info. This invalidation object contains information
   * such as the tables used by the materialized view, whether there was any update or
   * delete in the source tables since the materialized view was created or rebuilt,
   * or the invalidation time, i.e., first modification of the tables used by materialized
   * view after the view was created. */
  private final ConcurrentMap<String, ConcurrentMap<String, Materialization>> materializations =
      new ConcurrentHashMap<>();

  /*
   * Key is a qualified table name. The value is a (sorted) tree map (supporting concurrent
   * modifications) that will keep the modifications for a given table in the order of their
   * transaction id. This is useful to quickly check the invalidation time for a given
   * materialization.
   */
  private final ConcurrentMap<String, ConcurrentSkipListMap<Long, Long>> tableModifications =
      new ConcurrentHashMap<>();

  private final ConcurrentMap<String, ConcurrentSkipListSet<Long>> updateDeleteTableModifications =
      new ConcurrentHashMap<>();

  /* Whether the cache has been initialized or not. */
  private boolean initialized;
  /* Configuration for cache. */
  private Configuration conf;
  /* Handler to connect to metastore. */
  private IHMSHandler handler;

  private MaterializationsInvalidationCache() {
  }

  /**
   * Get instance of MaterializationsInvalidationCache.
   *
   * @return the singleton
   */
  public static MaterializationsInvalidationCache get() {
    return SINGLETON;
  }

  /**
   * Initialize the invalidation cache.
   *
   * The method is synchronized because we want to avoid initializing the invalidation cache
   * multiple times in embedded mode. This will not happen when we run the metastore remotely
   * as the method is called only once.
   */
  public synchronized void init(Configuration conf, IHMSHandler handler) {
    this.conf = conf;
    this.handler = handler;

    // This will only be true for debugging purposes
    this.disable = MetastoreConf.getVar(conf,
        MetastoreConf.ConfVars.MATERIALIZATIONS_INVALIDATION_CACHE_IMPL).equals("DISABLE");
    if (disable) {
      // Nothing to do
      return;
    }

    if (!initialized) {
      this.initialized = true;
      ExecutorService pool = Executors.newCachedThreadPool();
      pool.submit(new Loader());
      pool.shutdown();
    }
  }

  private class Loader implements Runnable {
    @Override
    public void run() {
      try {
        RawStore store = handler.getMS();
        for (String catName : store.getCatalogs()) {
          for (String dbName : store.getAllDatabases(catName)) {
            for (Table mv : store.getTableObjectsByName(catName, dbName,
                store.getTables(catName, dbName, null, TableType.MATERIALIZED_VIEW))) {
              addMaterializedView(mv.getDbName(), mv.getTableName(), ImmutableSet.copyOf(mv.getCreationMetadata().getTablesUsed()),
                  mv.getCreationMetadata().getValidTxnList(), OpType.LOAD);
            }
          }
        }
        LOG.info("Initialized materializations invalidation cache");
      } catch (Exception e) {
        LOG.error("Problem connecting to the metastore when initializing the view registry");
      }
    }
  }

  /**
   * Adds a newly created materialized view to the cache.
   *
   * @param dbName
   * @param tableName
   * @param tablesUsed tables used by the materialized view
   * @param validTxnList
   */
  public void createMaterializedView(String dbName, String tableName, Set<String> tablesUsed,
      String validTxnList) {
    addMaterializedView(dbName, tableName, tablesUsed, validTxnList, OpType.CREATE);
  }

  /**
   * Method to call when materialized view is modified.
   *
   * @param dbName
   * @param tableName
   * @param tablesUsed tables used by the materialized view
   * @param validTxnList
   */
  public void alterMaterializedView(String dbName, String tableName, Set<String> tablesUsed,
      String validTxnList) {
    addMaterializedView(dbName, tableName, tablesUsed, validTxnList, OpType.ALTER);
  }

  /**
   * Adds the materialized view to the cache.
   *
   * @param dbName
   * @param tableName
   * @param tablesUsed tables used by the materialized view
   * @param validTxnList
   * @param opType
   */
  private void addMaterializedView(String dbName, String tableName, Set<String> tablesUsed,
      String validTxnList, OpType opType) {
    if (disable) {
      // Nothing to do
      return;
    }
    // We are going to create the map for each view in the given database
    ConcurrentMap<String, Materialization> cq =
        new ConcurrentHashMap<String, Materialization>();
    final ConcurrentMap<String, Materialization> prevCq = materializations.putIfAbsent(
        dbName, cq);
    if (prevCq != null) {
      cq = prevCq;
    }
    // Start the process to add materialization to the cache
    // Before loading the materialization in the cache, we need to update some
    // important information in the registry to account for rewriting invalidation
    if (validTxnList == null) {
      // This can happen when the materialized view was created on non-transactional tables
      return;
    }
    if (opType == OpType.CREATE || opType == OpType.ALTER) {
      // You store the materialized view
      Materialization materialization = new Materialization(tablesUsed);
      materialization.setValidTxnList(validTxnList);
      cq.put(tableName, materialization);
    } else {
      ValidTxnWriteIdList txnList = new ValidTxnWriteIdList(validTxnList);
      for (String qNameTableUsed : tablesUsed) {
        ValidWriteIdList tableTxnList = txnList.getTableValidWriteIdList(qNameTableUsed);
        // First we insert a new tree set to keep table modifications, unless it already exists
        ConcurrentSkipListMap<Long, Long> modificationsTree = new ConcurrentSkipListMap<>();
        final ConcurrentSkipListMap<Long, Long> prevModificationsTree = tableModifications.putIfAbsent(
                qNameTableUsed, modificationsTree);
        if (prevModificationsTree != null) {
          modificationsTree = prevModificationsTree;
        }
        // If we are not creating the MV at this instant, but instead it was created previously
        // and we are loading it into the cache, we need to go through the transaction entries and
        // check if the MV is still valid.
        try {
          String[] names =  qNameTableUsed.split("\\.");
          BasicTxnInfo e = handler.getTxnHandler().getFirstCompletedTransactionForTableAfterCommit(
                  names[0], names[1], tableTxnList);
          if (!e.isIsnull()) {
            modificationsTree.put(e.getTxnid(), e.getTime());
            // We do not need to do anything more for current table, as we detected
            // a modification event that was in the metastore.
            continue;
          }
        } catch (MetaException ex) {
          LOG.debug("Materialized view " + Warehouse.getQualifiedName(dbName, tableName) +
                  " ignored; error loading view into invalidation cache", ex);
          return;
        }
      }
      // For LOAD, you only add it if it does exist as you might be loading an outdated MV
      Materialization materialization = new Materialization(tablesUsed);
      materialization.setValidTxnList(validTxnList);
      cq.putIfAbsent(tableName, materialization);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cached materialized view for rewriting in invalidation cache: " +
          Warehouse.getQualifiedName(dbName, tableName));
    }
  }

  /**
   * This method is called when a table is modified. That way we can keep track of the
   * invalidation for the MVs that use that table.
   */
  public void notifyTableModification(String dbName, String tableName,
      long txnId, long newModificationTime, boolean isUpdateDelete) {
    if (disable) {
      // Nothing to do
      return;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Notification for table {} in database {} received -> id: {}, time: {}",
          tableName, dbName, txnId, newModificationTime);
    }
    if (isUpdateDelete) {
      // We update first the update/delete modifications record
      ConcurrentSkipListSet<Long> modificationsSet = new ConcurrentSkipListSet<>();
      final ConcurrentSkipListSet<Long> prevModificationsSet =
          updateDeleteTableModifications.putIfAbsent(Warehouse.getQualifiedName(dbName, tableName),
              modificationsSet);
      if (prevModificationsSet != null) {
        modificationsSet = prevModificationsSet;
      }
      modificationsSet.add(txnId);
    }
    ConcurrentSkipListMap<Long, Long> modificationsTree = new ConcurrentSkipListMap<>();
    final ConcurrentSkipListMap<Long, Long> prevModificationsTree =
        tableModifications.putIfAbsent(Warehouse.getQualifiedName(dbName, tableName), modificationsTree);
    if (prevModificationsTree != null) {
      modificationsTree = prevModificationsTree;
    }
    modificationsTree.put(txnId, newModificationTime);
  }

  /**
   * Removes the materialized view from the cache.
   *
   * @param dbName
   * @param tableName
   */
  public void dropMaterializedView(String dbName, String tableName) {
    if (disable) {
      // Nothing to do
      return;
    }
    materializations.get(dbName).remove(tableName);
  }

  /**
   * Returns the materialized views in the cache for the given database.
   *
   * @param dbName the database
   * @return the collection of materialized views, or the empty collection if none
   */
  public Map<String, Materialization> getMaterializationInvalidationInfo(
      String dbName, List<String> materializationNames) {
    if (materializations.get(dbName) != null) {
      ImmutableMap.Builder<String, Materialization> m = ImmutableMap.builder();
      for (String materializationName : materializationNames) {
        Materialization materialization =
            materializations.get(dbName).get(materializationName);
        if (materialization == null) {
          LOG.debug("Materialization {} skipped as there is no information "
              + "in the invalidation cache about it", materializationName);
          continue;
        }
        // We create a deep copy of the materialization, as we need to set the time
        // and whether any update/delete operation happen on the tables that it uses
        // since it was created.
        Materialization materializationCopy = new Materialization(
            materialization.getTablesUsed());
        materializationCopy.setValidTxnList(materialization.getValidTxnList());
        enrichWithInvalidationInfo(materializationCopy);
        m.put(materializationName, materializationCopy);
      }
      Map<String, Materialization> result = m.build();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Retrieved the following materializations from the invalidation cache: {}", result);
      }
      return result;
    }
    return ImmutableMap.of();
  }

  private void enrichWithInvalidationInfo(Materialization materialization) {
    String materializationTxnListString = materialization.getValidTxnList();
    if (materializationTxnListString == null) {
      // This can happen when the materialization was created on non-transactional tables
      materialization.setInvalidationTime(Long.MIN_VALUE);
      return;
    }

    // We will obtain the modification time as follows.
    // First, we obtain the first element after high watermark (if any)
    // Then, we iterate through the elements from min open txn till high
    // watermark, updating the modification time after creation if needed
    ValidTxnWriteIdList materializationTxnList = new ValidTxnWriteIdList(materializationTxnListString);
    long firstModificationTimeAfterCreation = 0L;
    boolean containsUpdateDelete = false;
    for (String qNameTableUsed : materialization.getTablesUsed()) {
      final ValidWriteIdList tableMaterializationTxnList =
          materializationTxnList.getTableValidWriteIdList(qNameTableUsed);

      final ConcurrentSkipListMap<Long, Long> usedTableModifications =
          tableModifications.get(qNameTableUsed);
      if (usedTableModifications == null) {
        // This is not necessarily an error, since the table may be empty. To be safe,
        // instead of including this materialized view, we just log the information and
        // skip it (if table is really empty, it will not matter for performance anyway).
        LOG.warn("No information found in invalidation cache for table {}, possible tables are: {}",
            qNameTableUsed, tableModifications.keySet());
        materialization.setInvalidationTime(Long.MIN_VALUE);
        return;
      }
      final ConcurrentSkipListSet<Long> usedUDTableModifications =
          updateDeleteTableModifications.get(qNameTableUsed);
      final Entry<Long, Long> tn = usedTableModifications.higherEntry(tableMaterializationTxnList.getHighWatermark());
      if (tn != null) {
        if (firstModificationTimeAfterCreation == 0L ||
            tn.getValue() < firstModificationTimeAfterCreation) {
          firstModificationTimeAfterCreation = tn.getValue();
        }
        // Check if there was any update/delete after creation
        containsUpdateDelete = usedUDTableModifications != null &&
            !usedUDTableModifications.tailSet(tableMaterializationTxnList.getHighWatermark(), false).isEmpty();
      }
      // Min open txn might be null if there were no open transactions
      // when this transaction was being executed
      if (tableMaterializationTxnList.getMinOpenWriteId() != null) {
        // Invalid transaction list is sorted
        int pos = 0;
        for (Map.Entry<Long, Long> t : usedTableModifications
                .subMap(tableMaterializationTxnList.getMinOpenWriteId(), tableMaterializationTxnList.getHighWatermark()).entrySet()) {
          while (pos < tableMaterializationTxnList.getInvalidWriteIds().length &&
              tableMaterializationTxnList.getInvalidWriteIds()[pos] != t.getKey()) {
            pos++;
          }
          if (pos >= tableMaterializationTxnList.getInvalidWriteIds().length) {
            break;
          }
          if (firstModificationTimeAfterCreation == 0L ||
              t.getValue() < firstModificationTimeAfterCreation) {
            firstModificationTimeAfterCreation = t.getValue();
          }
          containsUpdateDelete = containsUpdateDelete ||
              (usedUDTableModifications != null && usedUDTableModifications.contains(t.getKey()));
        }
      }
    }

    materialization.setInvalidationTime(firstModificationTimeAfterCreation);
    materialization.setSourceTablesUpdateDeleteModified(containsUpdateDelete);
  }

  private enum OpType {
    CREATE,
    LOAD,
    ALTER
  }

  /**
   * Removes transaction events that are not relevant anymore.
   * @param minTime events generated before this time (ms) can be deleted from the cache
   * @return number of events that were deleted from the cache
   */
  public long cleanup(long minTime) {
    // To remove, mv should meet two conditions:
    // 1) Current time - time of transaction > config parameter, and
    // 2) Transaction should not be associated with invalidation of a MV
    if (disable || !initialized) {
      // Bail out
      return 0L;
    }
    // We execute the cleanup in two steps
    // First we gather all the transactions that need to be kept
    final Multimap<String, Long> keepTxnInfos = HashMultimap.create();
    for (Map.Entry<String, ConcurrentMap<String, Materialization>> e : materializations.entrySet()) {
      for (Materialization m : e.getValue().values()) {
        ValidTxnWriteIdList txnList = new ValidTxnWriteIdList(m.getValidTxnList());
        boolean canBeDeleted = false;
        String currentTableForInvalidatingTxn = null;
        long currentInvalidatingTxnId = 0L;
        long currentInvalidatingTxnTime = 0L;
        for (String qNameTableUsed : m.getTablesUsed()) {
          ValidWriteIdList tableTxnList = txnList.getTableValidWriteIdList(qNameTableUsed);
          final Entry<Long, Long> tn = tableModifications.get(qNameTableUsed)
              .higherEntry(tableTxnList.getHighWatermark());
          if (tn != null) {
            if (currentInvalidatingTxnTime == 0L ||
                tn.getValue() < currentInvalidatingTxnTime) {
              // This transaction 1) is the first one examined for this materialization, or
              // 2) it is the invalidating transaction. Hence we add it to the transactions to keep.
              // 1.- We remove the previous invalidating transaction from the transactions
              // to be kept (if needed).
              if (canBeDeleted && currentInvalidatingTxnTime < minTime) {
                keepTxnInfos.remove(currentTableForInvalidatingTxn, currentInvalidatingTxnId);
              }
              // 2.- We add this transaction to the transactions that should be kept.
              canBeDeleted = !keepTxnInfos.get(qNameTableUsed).contains(tn.getKey());
              keepTxnInfos.put(qNameTableUsed, tn.getKey());
              // 3.- We record this transaction as the current invalidating transaction.
              currentTableForInvalidatingTxn = qNameTableUsed;
              currentInvalidatingTxnId = tn.getKey();
              currentInvalidatingTxnTime = tn.getValue();
            }
          }
          if (tableTxnList.getMinOpenWriteId() != null) {
            // Invalid transaction list is sorted
            int pos = 0;
            for (Entry<Long, Long> t : tableModifications.get(qNameTableUsed)
                .subMap(tableTxnList.getMinOpenWriteId(), tableTxnList.getHighWatermark()).entrySet()) {
              while (pos < tableTxnList.getInvalidWriteIds().length &&
                  tableTxnList.getInvalidWriteIds()[pos] != t.getKey()) {
                pos++;
              }
              if (pos >= tableTxnList.getInvalidWriteIds().length) {
                break;
              }
              if (currentInvalidatingTxnTime == 0L ||
                  t.getValue() < currentInvalidatingTxnTime) {
                // This transaction 1) is the first one examined for this materialization, or
                // 2) it is the invalidating transaction. Hence we add it to the transactions to keep.
                // 1.- We remove the previous invalidating transaction from the transactions
                // to be kept (if needed).
                if (canBeDeleted && currentInvalidatingTxnTime < minTime) {
                  keepTxnInfos.remove(currentTableForInvalidatingTxn, currentInvalidatingTxnId);
                }
                // 2.- We add this transaction to the transactions that should be kept.
                canBeDeleted = !keepTxnInfos.get(qNameTableUsed).contains(t.getKey());
                keepTxnInfos.put(qNameTableUsed, t.getKey());
                // 3.- We record this transaction as the current invalidating transaction.
                currentTableForInvalidatingTxn = qNameTableUsed;
                currentInvalidatingTxnId = t.getKey();
                currentInvalidatingTxnTime = t.getValue();
              }
            }
          }
        }
      }
    }
    // Second, we remove the transactions
    long removed = 0L;
    for (Entry<String, ConcurrentSkipListMap<Long, Long>> e : tableModifications.entrySet()) {
      Collection<Long> c = keepTxnInfos.get(e.getKey());
      ConcurrentSkipListSet<Long> updateDeleteForTable = updateDeleteTableModifications.get(e.getKey());
      for (Iterator<Entry<Long, Long>> it = e.getValue().entrySet().iterator(); it.hasNext();) {
        Entry<Long, Long> v = it.next();
        // We need to check again the time because some of the transactions might not be explored
        // above, e.g., transactions above the highest transaction mark for all the materialized
        // views.
        if (v.getValue() < minTime && (c.isEmpty() || !c.contains(v.getKey()))) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Transaction removed from cache for table {} -> id: {}, time: {}",
                e.getKey(), v.getKey(), v.getValue());
          }
          if (updateDeleteForTable != null) {
            updateDeleteForTable.remove(v.getKey());
          }
          it.remove();
          removed++;
        }
      }
    }
    return removed;
  }

  /**
   * Checks whether the given materialization exists in the invalidation cache.
   * @param dbName the database name for the materialization
   * @param tblName the table name for the materialization
   * @return true if we have information about the materialization in the cache,
   * false otherwise
   */
  public boolean containsMaterialization(String dbName, String tblName) {
    if (disable || dbName == null || tblName == null) {
      return false;
    }
    ConcurrentMap<String, Materialization> dbMaterializations = materializations.get(dbName);
    if (dbMaterializations == null || dbMaterializations.get(tblName) == null) {
      // This is a table
      return false;
    }
    return true;
  }

}
