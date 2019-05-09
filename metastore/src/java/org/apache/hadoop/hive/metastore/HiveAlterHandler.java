/**
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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.ReplConst;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hive.common.util.HiveStringUtils;

import com.google.common.collect.Lists;

import static org.apache.hadoop.hive.metastore.HiveMetaHook.ALTERLOCATION;
import static org.apache.hadoop.hive.metastore.HiveMetaHook.ALTER_TABLE_OPERATION_TYPE;

/**
 * Hive specific implementation of alter
 */
public class HiveAlterHandler implements AlterHandler {

  protected Configuration hiveConf;
  private static final Log LOG = LogFactory.getLog(HiveAlterHandler.class
      .getName());

  @Override
  public Configuration getConf() {
    return hiveConf;
  }

  @Override
  @SuppressWarnings("nls")
  public void setConf(Configuration conf) {
    hiveConf = conf;
  }

  @Override
  public void alterTable(RawStore msdb, Warehouse wh, String dbname,
    String name, Table newt, EnvironmentContext environmentContext)
      throws InvalidOperationException, MetaException {
    alterTable(msdb, wh, dbname, name, newt, environmentContext, null);
  }

  @Override
  public void alterTable(RawStore msdb, Warehouse wh, String dbname,
      String name, Table newt, EnvironmentContext environmentContext,
      HMSHandler handler) throws InvalidOperationException, MetaException {
    final boolean cascade;
    final boolean replDataLocationChanged;
    if ((environmentContext != null) && environmentContext.isSetProperties()) {
      cascade = StatsSetupConst.TRUE.equals(environmentContext.getProperties().get(StatsSetupConst.CASCADE));
      replDataLocationChanged = ReplConst.TRUE.equals(environmentContext.getProperties().get(ReplConst.REPL_DATA_LOCATION_CHANGED));
    } else {
      cascade = false;
      replDataLocationChanged = false;
    }

    if (newt == null) {
      throw new InvalidOperationException("New table is invalid: " + newt);
    }

    if (!MetaStoreUtils.validateName(newt.getTableName())) {
      throw new InvalidOperationException(newt.getTableName()
          + " is not a valid object name");
    }
    String validate = MetaStoreUtils.validateTblColumns(newt.getSd().getCols(), hiveConf);
    if (validate != null) {
      throw new InvalidOperationException("Invalid column " + validate);
    }

    Path srcPath = null;
    FileSystem srcFs = null;
    Path destPath = null;
    FileSystem destFs = null;

    boolean success = false;
    boolean dataWasMoved = false;
    boolean rename = false;
    boolean isPartitionedTable = false;

    Database olddb = null;
    Table oldt = null;
    List<ObjectPair<Partition, String>> altps = new ArrayList<ObjectPair<Partition, String>>();
    List<MetaStoreEventListener> transactionalListeners = null;
    if (handler != null) {
      transactionalListeners = handler.getTransactionalListeners();
    }

    try {
      msdb.openTransaction();
      name = name.toLowerCase();
      dbname = dbname.toLowerCase();

      // check if table with the new name already exists
      if (!newt.getTableName().equalsIgnoreCase(name)
          || !newt.getDbName().equalsIgnoreCase(dbname)) {
        if (msdb.getTable(newt.getDbName(), newt.getTableName()) != null) {
          throw new InvalidOperationException("new table " + newt.getDbName()
              + "." + newt.getTableName() + " already exists");
        }
        rename = true;
      }

      // get old table
      olddb = msdb.getDatabase(dbname);
      oldt = msdb.getTable(dbname, name);
      if (oldt == null) {
        throw new InvalidOperationException("table " + newt.getDbName() + "."
            + newt.getTableName() + " doesn't exist");
      }

      validateTableChangesOnReplSource(olddb, oldt, newt, environmentContext);

      if (oldt.getPartitionKeysSize() != 0) {
        isPartitionedTable = true;
      }

      if (HiveConf.getBoolVar(hiveConf,
            HiveConf.ConfVars.METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES,
            false)) {
        // Throws InvalidOperationException if the new column types are not
        // compatible with the current column types.
        MetaStoreUtils.throwExceptionIfIncompatibleColTypeChange(
            oldt.getSd().getCols(), newt.getSd().getCols());
      }

      if (cascade) {
        //Currently only column related changes can be cascaded in alter table
        if(MetaStoreUtils.isCascadeNeededInAlterTable(oldt, newt)) {
          List<Partition> parts = msdb.getPartitions(dbname, name, -1);
          for (Partition part : parts) {
            List<FieldSchema> oldCols = part.getSd().getCols();
            part.getSd().setCols(newt.getSd().getCols());
            String oldPartName = Warehouse.makePartName(oldt.getPartitionKeys(), part.getValues());
            updatePartColumnStatsForAlterColumns(msdb, part, oldPartName, part.getValues(), oldCols, part);
            msdb.alterPartition(dbname, name, part.getValues(), part);
          }
        } else {
          LOG.warn("Alter table does not cascade changes to its partitions.");
        }
      }

      //check that partition keys have not changed, except for virtual views
      //however, allow the partition comments to change
      boolean partKeysPartiallyEqual = checkPartialPartKeysEqual(oldt.getPartitionKeys(),
          newt.getPartitionKeys());

      if(!oldt.getTableType().equals(TableType.VIRTUAL_VIEW.toString())){
        if (oldt.getPartitionKeys().size() != newt.getPartitionKeys().size()
            || !partKeysPartiallyEqual) {
          throw new InvalidOperationException(
              "partition keys can not be changed.");
        }
      }

      // Two mutually exclusive flows possible.
      // i) Partition locations needs update if replDataLocationChanged is true which means table's
      // data location is changed with all partition sub-directories.
      // ii) Rename needs change the data location and move the data to the new location corresponding
      // to the new name if:
      // 1) the table is not a virtual view, and
      // 2) the table is not an external table, and
      // 3) the user didn't change the default location (or new location is empty), and
      // 4) the table was not initially created with a specified location
      if (replDataLocationChanged
              || (rename
              && !oldt.getTableType().equals(TableType.VIRTUAL_VIEW.toString())
              && (oldt.getSd().getLocation().compareTo(newt.getSd().getLocation()) == 0
              || StringUtils.isEmpty(newt.getSd().getLocation()))
              && !MetaStoreUtils.isExternalTable(oldt))) {
        srcPath = new Path(oldt.getSd().getLocation());

        if (replDataLocationChanged) {
          // If data location is changed in replication flow, then new path was already set in
          // the newt. Also, it is as good as the data is moved and set dataWasMoved=true so that
          // location in partitions are also updated accordingly.
          // No need to validate if the destPath exists as in replication flow, data gets replicated
          // separately.
          destPath = new Path(newt.getSd().getLocation());
          dataWasMoved = true;
        } else {
          // Rename flow.
          srcFs = wh.getFs(srcPath);

          // that means user is asking metastore to move data to new location
          // corresponding to the new name
          // get new location
          Database db = msdb.getDatabase(newt.getDbName());
          Path databasePath = constructRenamedPath(wh.getDatabasePath(db), srcPath);
          destPath = new Path(databasePath, newt.getTableName().toLowerCase());
          destFs = wh.getFs(destPath);

          newt.getSd().setLocation(destPath.toString());

          // check that destination does not exist otherwise we will be
          // overwriting data
          // check that src and dest are on the same file system
          if (!FileUtils.equalsFileSystem(srcFs, destFs)) {
            throw new InvalidOperationException("table new location " + destPath
                    + " is on a different file system than the old location "
                    + srcPath + ". This operation is not supported");
          }
          try {
            srcFs.exists(srcPath); // check that src exists and also checks
            // permissions necessary
            if (destFs.exists(destPath)) {
              throw new InvalidOperationException("New location for this table "
                      + newt.getDbName() + "." + newt.getTableName()
                      + " already exists : " + destPath);
            }
            // check that src exists and also checks permissions necessary, rename src to dest
            if (srcFs.exists(srcPath) && wh.renameDir(srcPath, destPath,
                    ReplChangeManager.isSourceOfReplication(msdb.getDatabase(dbname)))) {
              dataWasMoved = true;
            }
          } catch (IOException | MetaException e) {
            LOG.error("Alter Table operation for " + dbname + "." + name + " failed.", e);
            throw new InvalidOperationException("Alter Table operation for " + dbname + "." + name +
                    " failed to move data due to: '" + getSimpleMessage(e)
                    + "' See hive log file for details.");
          }
        }

        String oldTblLocPath = srcPath.toUri().getPath();
        String newTblLocPath = dataWasMoved ? destPath.toUri().getPath() : null;

        // also the location field in partition
        List<Partition> parts = msdb.getPartitions(dbname, name, -1);
        for (Partition part : parts) {
          String oldPartLoc = part.getSd().getLocation();
          if (dataWasMoved && oldPartLoc.contains(oldTblLocPath)) {
            URI oldUri = new Path(oldPartLoc).toUri();
            String newPath = oldUri.getPath().replace(oldTblLocPath, newTblLocPath);
            Path newPartLocPath = new Path(oldUri.getScheme(), oldUri.getAuthority(), newPath);
            altps.add(ObjectPair.create(part, part.getSd().getLocation()));
            part.getSd().setLocation(newPartLocPath.toString());
            String oldPartName = Warehouse.makePartName(oldt.getPartitionKeys(), part.getValues());
            try {
              //existing partition column stats is no longer valid, remove them
              msdb.deletePartitionColumnStatistics(dbname, name, oldPartName, part.getValues(), null);
            } catch (InvalidInputException iie) {
              throw new InvalidOperationException("Unable to update partition stats in table rename." + iie);
            }
            msdb.alterPartition(dbname, name, part.getValues(), part);
          }
        }

      } else if (MetaStoreUtils.requireCalStats(hiveConf, null, null, newt, environmentContext) &&
        (newt.getPartitionKeysSize() == 0)) {
          Database db = msdb.getDatabase(newt.getDbName());
          // Update table stats. For partitioned table, we update stats in
          // alterPartition()
          MetaStoreUtils.updateTableStatsFast(db, newt, wh, false, true, environmentContext);
      }
      updateTableColumnStatsForAlterTable(msdb, oldt, newt);
      // now finally call alter table
      msdb.alterTable(dbname, name, newt);
      if (transactionalListeners != null && !transactionalListeners.isEmpty()) {
        if (oldt.getDbName().equalsIgnoreCase(newt.getDbName())) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                  EventMessage.EventType.ALTER_TABLE,
                  new AlterTableEvent(oldt, newt, false, true, handler),
                  environmentContext);
        } else {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                  EventMessage.EventType.DROP_TABLE,
                  new DropTableEvent(oldt, true, false, handler),
                  environmentContext);
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                  EventMessage.EventType.CREATE_TABLE,
                  new CreateTableEvent(newt, true, handler),
                  environmentContext);
          if (isPartitionedTable) {
            List<Partition> parts = msdb.getPartitions(newt.getDbName(), newt.getTableName(), -1);
            MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                    EventMessage.EventType.ADD_PARTITION,
                    new AddPartitionEvent(newt, parts, true, handler),
                    environmentContext);
          }
        }
      }
      // commit the changes
      success = msdb.commitTransaction();
    } catch (InvalidObjectException e) {
      LOG.debug(e);
      throw new InvalidOperationException(
          "Unable to change partition or table."
              + " Check metastore logs for detailed stack." + e.getMessage());
    } catch (NoSuchObjectException e) {
      LOG.debug(e);
      throw new InvalidOperationException(
          "Unable to change partition or table. Database " + dbname + " does not exist"
              + " Check metastore logs for detailed stack." + e.getMessage());
    } finally {
      if (success) {
        // Txn was committed successfully.
        // If data location is changed in replication flow, then need to delete the old path.
        if (replDataLocationChanged) {
          assert(olddb != null);
          assert(oldt != null);
          Path deleteOldDataLoc = new Path(oldt.getSd().getLocation());
          boolean isAutoPurge = "true".equalsIgnoreCase(oldt.getParameters().get("auto.purge"));
          try {
            wh.deleteDir(deleteOldDataLoc, true, isAutoPurge, olddb);
            LOG.info("Deleted the old data location: " + deleteOldDataLoc + " for the table: "
                    + dbname + "." + name);
          } catch (MetaException ex) {
            // Eat the exception as it doesn't affect the state of existing tables.
            // Expect, user to manually drop this path when exception and so logging a warning.
            LOG.warn("Unable to delete the old data location: " + deleteOldDataLoc + " for the table: "
                    + dbname + "." + name);
          }
        }
      } else {
        msdb.rollbackTransaction();
        if (!replDataLocationChanged && dataWasMoved) {
          try {
            if (destFs.exists(destPath)) {
              if (!destFs.rename(destPath, srcPath)) {
                LOG.error("Failed to restore data from " + destPath + " to " + srcPath
                    + " in alter table failure. Manual restore is needed.");
              }
            }
          } catch (IOException e) {
            LOG.error("Failed to restore data from " + destPath + " to " + srcPath
                +  " in alter table failure. Manual restore is needed.");
          }
        }
      }
    }
  }

  /**
   * MetaException that encapsulates error message from RemoteException from hadoop RPC which wrap
   * the stack trace into e.getMessage() which makes logs/stack traces confusing.
   * @param ex
   * @return
   */
  String getSimpleMessage(Exception ex) {
    if(ex instanceof MetaException) {
      String msg = ex.getMessage();
      if(msg == null || !msg.contains("\n")) {
        return msg;
      }
      return msg.substring(0, msg.indexOf('\n'));
    }
    return ex.getMessage();
  }

  @Override
  public Partition alterPartition(final RawStore msdb, Warehouse wh, final String dbname,
    final String name, final List<String> part_vals, final Partition new_part,
    EnvironmentContext environmentContext)
      throws InvalidOperationException, InvalidObjectException, AlreadyExistsException, MetaException {
    return alterPartition(msdb, wh, dbname, name, part_vals, new_part, environmentContext, null);
  }

  @Override
  public Partition alterPartition(final RawStore msdb, Warehouse wh, final String dbname,
    final String name, final List<String> part_vals, final Partition new_part,
    EnvironmentContext environmentContext, HMSHandler handler)
      throws InvalidOperationException, InvalidObjectException, AlreadyExistsException, MetaException {
    boolean success = false;
    Path srcPath = null;
    Path destPath = null;
    FileSystem srcFs = null;
    FileSystem destFs;
    Partition oldPart = null;
    String oldPartLoc = null;
    String newPartLoc = null;
    Database db = null;

    List<MetaStoreEventListener> transactionalListeners = null;
    if (handler != null) {
      transactionalListeners = handler.getTransactionalListeners();
    }

    // Set DDL time to now if not specified
    if (new_part.getParameters() == null ||
        new_part.getParameters().get(hive_metastoreConstants.DDL_TIME) == null ||
        Integer.parseInt(new_part.getParameters().get(hive_metastoreConstants.DDL_TIME)) == 0) {
      new_part.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(System
          .currentTimeMillis() / 1000));
    }

    Table tbl = msdb.getTable(dbname, name);
    if (tbl == null) {
      throw new InvalidObjectException(
          "Unable to alter partition because table or database does not exist.");
    }

    //alter partition
    if (part_vals == null || part_vals.size() == 0) {
      try {
        msdb.openTransaction();
        oldPart = msdb.getPartition(dbname, name, new_part.getValues());
        if (MetaStoreUtils.requireCalStats(hiveConf, oldPart, new_part, tbl, environmentContext)) {
          MetaStoreUtils.updatePartitionStatsFast(new_part, wh, false, true, environmentContext);
        }

        updatePartColumnStats(msdb, dbname, name, new_part.getValues(), new_part);
        msdb.alterPartition(dbname, name, new_part.getValues(), new_part);
        if (transactionalListeners != null && !transactionalListeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                                EventMessage.EventType.ALTER_PARTITION,
                                                new AlterPartitionEvent(oldPart, new_part, tbl, false, true, handler),
                                                environmentContext);
        }
        success = msdb.commitTransaction();
      } catch (InvalidObjectException e) {
        throw new InvalidOperationException("alter is not possible");
      } catch (NoSuchObjectException e){
        //old partition does not exist
        throw new InvalidOperationException("alter is not possible");
      } finally {
        if(!success) {
          msdb.rollbackTransaction();
        }
      }
      return oldPart;
    }

    //rename partition
    try {
      msdb.openTransaction();
      try {
        oldPart = msdb.getPartition(dbname, name, part_vals);
      } catch (NoSuchObjectException e) {
        // this means there is no existing partition
        throw new InvalidObjectException(
            "Unable to rename partition because old partition does not exist");
      }

      Partition check_part;
      try {
        check_part = msdb.getPartition(dbname, name, new_part.getValues());
      } catch(NoSuchObjectException e) {
        // this means there is no existing partition
        check_part = null;
      }

      if (check_part != null) {
        throw new AlreadyExistsException("Partition already exists:" + dbname + "." + name + "." +
            new_part.getValues());
      }

      // if the external partition is renamed, the file should not change
      if (tbl.getTableType().equals(TableType.EXTERNAL_TABLE.toString())) {
        new_part.getSd().setLocation(oldPart.getSd().getLocation());
        String oldPartName = Warehouse.makePartName(tbl.getPartitionKeys(), oldPart.getValues());
        try {
          //existing partition column stats is no longer valid, remove
          msdb.deletePartitionColumnStatistics(dbname, name, oldPartName, oldPart.getValues(), null);
        } catch (NoSuchObjectException nsoe) {
          //ignore
        } catch (InvalidInputException iie) {
          throw new InvalidOperationException("Unable to update partition stats in table rename." + iie);
        }
        msdb.alterPartition(dbname, name, part_vals, new_part);
      } else {
        try {
          db = msdb.getDatabase(dbname);
          // if tbl location is available use it
          // else derive the tbl location from database location
          destPath = wh.getPartitionPath(db, tbl, new_part.getValues());
          destPath = constructRenamedPath(destPath, new Path(new_part.getSd().getLocation()));
        } catch (NoSuchObjectException e) {
          LOG.debug(e);
          throw new InvalidOperationException(
            "Unable to change partition or table. Database " + dbname + " does not exist"
              + " Check metastore logs for detailed stack." + e.getMessage());
        }

        if (destPath != null) {
          newPartLoc = destPath.toString();
          oldPartLoc = oldPart.getSd().getLocation();
          LOG.info("srcPath:" + oldPartLoc);
          LOG.info("descPath:" + newPartLoc);
          srcPath = new Path(oldPartLoc);
          srcFs = wh.getFs(srcPath);
          destFs = wh.getFs(destPath);
          // check that src and dest are on the same file system
          if (!FileUtils.equalsFileSystem(srcFs, destFs)) {
            throw new InvalidOperationException("New table location " + destPath
              + " is on a different file system than the old location "
              + srcPath + ". This operation is not supported.");
          }

          try {
            srcFs.exists(srcPath);
            if (newPartLoc.compareTo(oldPartLoc) != 0 && destFs.exists(destPath)) {
              throw new InvalidOperationException("New location for this table "
                + tbl.getDbName() + "." + tbl.getTableName()
                + " already exists : " + destPath);
            }
          } catch (IOException e) {
            throw new InvalidOperationException("Unable to access new location "
              + destPath + " for partition " + tbl.getDbName() + "."
              + tbl.getTableName() + " " + new_part.getValues());
          }

          new_part.getSd().setLocation(newPartLoc);
          if (MetaStoreUtils.requireCalStats(hiveConf, oldPart, new_part, tbl, environmentContext)) {
            MetaStoreUtils.updatePartitionStatsFast(new_part, wh, false, true, environmentContext);
          }

          String oldPartName = Warehouse.makePartName(tbl.getPartitionKeys(), oldPart.getValues());
          try {
            //existing partition column stats is no longer valid, remove
            msdb.deletePartitionColumnStatistics(dbname, name, oldPartName, oldPart.getValues(), null);
          } catch (NoSuchObjectException nsoe) {
            //ignore
          } catch (InvalidInputException iie) {
            throw new InvalidOperationException("Unable to update partition stats in table rename." + iie);
          }

          msdb.alterPartition(dbname, name, part_vals, new_part);
        }
      }

      if (transactionalListeners != null && !transactionalListeners.isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                              EventMessage.EventType.ALTER_PARTITION,
                                              new AlterPartitionEvent(oldPart, new_part, tbl, false, true, handler),
                                              environmentContext);
      }

      success = msdb.commitTransaction();
    } finally {
      if (!success) {
        msdb.rollbackTransaction();
      }

      if (success && newPartLoc != null && newPartLoc.compareTo(oldPartLoc) != 0) {
        //rename the data directory
        try{
          if (srcFs.exists(srcPath)) {
            //if destPath's parent path doesn't exist, we should mkdir it
            Path destParentPath = destPath.getParent();
            if (!wh.mkdirs(destParentPath, ReplChangeManager.isSourceOfReplication(db))) {
                throw new IOException("Unable to create path " + destParentPath);
            }

            wh.renameDir(srcPath, destPath, true, true);
            LOG.info("Partition directory rename from " + srcPath + " to " + destPath + " done.");
          }
        } catch (IOException | MetaException ex) {
          LOG.error("Cannot rename partition directory from " + srcPath + " to " +
              destPath, ex);
          boolean revertMetaDataTransaction = false;
          try {
            msdb.openTransaction();
            msdb.alterPartition(dbname, name, new_part.getValues(), oldPart);
            if (transactionalListeners != null && !transactionalListeners.isEmpty()) {
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                                    EventMessage.EventType.ALTER_PARTITION,
                                                    new AlterPartitionEvent(new_part, oldPart, tbl, false, success, handler),
                                                    environmentContext);
            }

            revertMetaDataTransaction = msdb.commitTransaction();
          } catch (Exception ex2) {
            LOG.error("Attempt to revert partition metadata change failed. The revert was attempted " +
                "because associated filesystem rename operation failed with exception " + ex.getMessage(), ex2);
            if (!revertMetaDataTransaction) {
              msdb.rollbackTransaction();
            }
          }

          throw new InvalidOperationException("Unable to access old location "
              + srcPath + " for partition " + tbl.getDbName() + "."
              + tbl.getTableName() + " " + part_vals);
        }
      }
    }
    return oldPart;
  }

  @Override
  public List<Partition> alterPartitions(final RawStore msdb, Warehouse wh, final String dbname,
    final String name, final List<Partition> new_parts,
    EnvironmentContext environmentContext)
      throws InvalidOperationException, InvalidObjectException, AlreadyExistsException, MetaException {
    return alterPartitions(msdb, wh, dbname, name, new_parts, environmentContext, null);
  }

  @Override
  public List<Partition> alterPartitions(final RawStore msdb, Warehouse wh, final String dbname,
    final String name, final List<Partition> new_parts, EnvironmentContext environmentContext,
    HMSHandler handler)
      throws InvalidOperationException, InvalidObjectException, AlreadyExistsException,
          MetaException {
    List<Partition> oldParts = new ArrayList<Partition>();
    List<List<String>> partValsList = new ArrayList<List<String>>();
    List<MetaStoreEventListener> transactionalListeners = null;
    if (handler != null) {
      transactionalListeners = handler.getTransactionalListeners();
    }

    Table tbl = msdb.getTable(dbname, name);
    if (tbl == null) {
      throw new InvalidObjectException(
          "Unable to alter partitions because table or database does not exist.");
    }

    try {
      blockPartitionLocationChangesOnReplSource(msdb.getDatabase(dbname), tbl, environmentContext);
    } catch (NoSuchObjectException e) {
      LOG.debug(e);
      throw new InvalidOperationException(
              "Unable to change partition or table. Database " + dbname + " does not exist"
                      + " Check metastore logs for detailed stack." + e.getMessage());
    }


    boolean success = false;
    try {
      msdb.openTransaction();
      for (Partition tmpPart: new_parts) {
        // Set DDL time to now if not specified
        if (tmpPart.getParameters() == null ||
            tmpPart.getParameters().get(hive_metastoreConstants.DDL_TIME) == null ||
            Integer.parseInt(tmpPart.getParameters().get(hive_metastoreConstants.DDL_TIME)) == 0) {
          tmpPart.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(System
              .currentTimeMillis() / 1000));
        }

        Partition oldTmpPart = msdb.getPartition(dbname, name, tmpPart.getValues());
        oldParts.add(oldTmpPart);
        partValsList.add(tmpPart.getValues());

        if (MetaStoreUtils.requireCalStats(hiveConf, oldTmpPart, tmpPart, tbl, environmentContext)) {
          MetaStoreUtils.updatePartitionStatsFast(tmpPart, wh, false, true, environmentContext);
        }
        updatePartColumnStats(msdb, dbname, name, oldTmpPart.getValues(), tmpPart);
      }
      msdb.alterPartitions(dbname, name, partValsList, new_parts);
      Iterator<Partition> oldPartsIt = oldParts.iterator();
      for (Partition newPart : new_parts) {
        Partition oldPart;
        if (oldPartsIt.hasNext()) {
          oldPart = oldPartsIt.next();
        } else {
          throw new InvalidOperationException(
              "Missing old partition corresponding to new partition "
                  + "when invoking MetaStoreEventListener for alterPartitions event.");
        }

        if (transactionalListeners != null && !transactionalListeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                                EventMessage.EventType.ALTER_PARTITION,
                                                new AlterPartitionEvent(oldPart, newPart, tbl, false, true, handler));
        }
      }

      success = msdb.commitTransaction();
    } catch (InvalidObjectException | NoSuchObjectException e) {
      throw new InvalidOperationException("Alter partition operation failed: " + e);
    } finally {
      if (!success) {
        msdb.rollbackTransaction();
      }
    }
    return oldParts;
  }

  // Validate changes to partition's location to protect against errors on migration during
  // replication
  private void blockPartitionLocationChangesOnReplSource(Database db, Table tbl,
                                                         EnvironmentContext ec)
          throws InvalidOperationException {
    // If the database is not replication source, nothing to do
    if (!ReplChangeManager.isSourceOfReplication(db)) {
      return;
    }

    // Do not allow changing location of a managed table as alter event doesn't capture the
    // new files list. So, it may cause data inconsistency.
    if (ec.isSetProperties()) {
      String alterType = ec.getProperties().get(ALTER_TABLE_OPERATION_TYPE);
      if (alterType != null && alterType.equalsIgnoreCase(ALTERLOCATION) &&
              tbl.getTableType().equalsIgnoreCase(TableType.MANAGED_TABLE.name())) {
        throw new InvalidOperationException("Cannot change location of a managed table " +
                Warehouse.getQualifiedName(tbl) + " as it is enabled for replication.");
      }
    }
  }

  // Validate changes to a table to protect against errors on migration during replication.
  private void validateTableChangesOnReplSource(Database db, Table oldTbl, Table newTbl,
                                                EnvironmentContext ec)
          throws InvalidOperationException {
    // If the database is not replication source, nothing to do
    if (!ReplChangeManager.isSourceOfReplication(db)) {
      return;
    }

    // Do not allow changing location of a managed table as alter event doesn't capture the
    // new files list. So, it may cause data inconsistency. We do this whether or not strict
    // managed is true on the source cluster.
    if (ec.isSetProperties()) {
      String alterType = ec.getProperties().get(ALTER_TABLE_OPERATION_TYPE);
      if (alterType != null && alterType.equalsIgnoreCase(ALTERLOCATION) &&
              oldTbl.getTableType().equalsIgnoreCase(TableType.MANAGED_TABLE.name())) {
        throw new InvalidOperationException("Cannot change location of a managed table " +
                Warehouse.getQualifiedName(oldTbl) + " as it is enabled for replication.");
      }
    }

    // Do not allow changing the type of table. This is to avoid migration scenarios which causes
    // Managed ACID table to be converted to external at replica. As ACID tables cannot be
    // converted to external table and vice versa, we need to restrict this conversion at primary
    // as well. Currently, table type conversion is allowed only between managed and external
    // table types. But, to be future proof, any table type conversion is restricted on a
    // replication enabled DB.
    if (!oldTbl.getTableType().equalsIgnoreCase(newTbl.getTableType())) {
      throw new InvalidOperationException("Table type cannot be changed from " + oldTbl.getTableType()
              + " to " + newTbl.getTableType() + " for the table " +
              oldTbl.getDbName() + "." + oldTbl.getTableName()
              + " as it is enabled for replication.");
    }

    // Also we do not allow changing a non-Acid managed table to acid table on source with strict
    // managed false. After replicating a non-acid managed table to a target with strict managed
    // true the table will be converted to acid or external table. So changing the transactional
    // property of table on source can conflict with resultant change in the target.
    if (!TxnUtils.isAcidTable(oldTbl) && TxnUtils.isAcidTable(newTbl)) {
      throw new InvalidOperationException("A non-Acid table cannot be converted to an Acid " +
              "table for the table " + Warehouse.getQualifiedName(oldTbl)
              + " as it is enabled for replication.");
    }
  }

  private boolean checkPartialPartKeysEqual(List<FieldSchema> oldPartKeys,
      List<FieldSchema> newPartKeys) {
    //return true if both are null, or false if one is null and the other isn't
    if (newPartKeys == null || oldPartKeys == null) {
      return oldPartKeys == newPartKeys;
    }
    if (oldPartKeys.size() != newPartKeys.size()) {
      return false;
    }
    Iterator<FieldSchema> oldPartKeysIter = oldPartKeys.iterator();
    Iterator<FieldSchema> newPartKeysIter = newPartKeys.iterator();
    FieldSchema oldFs;
    FieldSchema newFs;
    while (oldPartKeysIter.hasNext()) {
      oldFs = oldPartKeysIter.next();
      newFs = newPartKeysIter.next();
      // Alter table can change the type of partition key now.
      // So check the column name only.
      if (!oldFs.getName().equals(newFs.getName())) {
        return false;
      }
    }

    return true;
  }

  /**
   * Uses the scheme and authority of the object's current location and the path constructed
   * using the object's new name to construct a path for the object's new location.
   */
  private Path constructRenamedPath(Path defaultNewPath, Path currentPath) {
    URI currentUri = currentPath.toUri();

    return new Path(currentUri.getScheme(), currentUri.getAuthority(),
        defaultNewPath.toUri().getPath());
  }

  private void updatePartColumnStatsForAlterColumns(RawStore msdb, Partition oldPartition,
      String oldPartName, List<String> partVals, List<FieldSchema> oldCols, Partition newPart)
          throws MetaException, InvalidObjectException {
    String dbName = oldPartition.getDbName();
    String tableName = oldPartition.getTableName();
    try {
      List<String> oldPartNames = Lists.newArrayList(oldPartName);
      List<String> oldColNames = new ArrayList<String>(oldCols.size());
      for (FieldSchema oldCol : oldCols) {
        oldColNames.add(oldCol.getName());
      }
      List<FieldSchema> newCols = newPart.getSd().getCols();
      List<ColumnStatistics> partsColStats = msdb.getPartitionColumnStatistics(dbName, tableName,
          oldPartNames, oldColNames);
      assert (partsColStats.size() <= 1);
      for (ColumnStatistics partColStats : partsColStats) { //actually only at most one loop
        List<ColumnStatisticsObj> statsObjs = partColStats.getStatsObj();
        for (ColumnStatisticsObj statsObj : statsObjs) {
          boolean found =false;
          for (FieldSchema newCol : newCols) {
            if (statsObj.getColName().equals(newCol.getName())
                && statsObj.getColType().equals(newCol.getType())) {
              found = true;
              break;
            }
          }
          if (!found) {
            msdb.deletePartitionColumnStatistics(dbName, tableName, oldPartName, partVals,
                statsObj.getColName());
          }
        }
      }
    } catch (NoSuchObjectException nsoe) {
      LOG.debug("Could not find db entry." + nsoe);
      //ignore
    } catch (InvalidInputException iie) {
      throw new InvalidObjectException
      ("Invalid input to update partition column stats in alter table change columns" + iie);
    }
  }

  private void updatePartColumnStats(RawStore msdb, String dbName, String tableName,
      List<String> partVals, Partition newPart) throws MetaException, InvalidObjectException {
    dbName = HiveStringUtils.normalizeIdentifier(dbName);
    tableName = HiveStringUtils.normalizeIdentifier(tableName);
    String newDbName = HiveStringUtils.normalizeIdentifier(newPart.getDbName());
    String newTableName = HiveStringUtils.normalizeIdentifier(newPart.getTableName());

    Table oldTable = msdb.getTable(dbName, tableName);
    if (oldTable == null) {
      return;
    }

    try {
      String oldPartName = Warehouse.makePartName(oldTable.getPartitionKeys(), partVals);
      String newPartName = Warehouse.makePartName(oldTable.getPartitionKeys(), newPart.getValues());
      if (!dbName.equals(newDbName) || !tableName.equals(newTableName)
          || !oldPartName.equals(newPartName)) {
        msdb.deletePartitionColumnStatistics(dbName, tableName, oldPartName, partVals, null);
      } else {
        Partition oldPartition = msdb.getPartition(dbName, tableName, partVals);
        if (oldPartition == null) {
          return;
        }
        if (oldPartition.getSd() != null && newPart.getSd() != null) {
        List<FieldSchema> oldCols = oldPartition.getSd().getCols();
          if (!MetaStoreUtils.areSameColumns(oldCols, newPart.getSd().getCols())) {
            updatePartColumnStatsForAlterColumns(msdb, oldPartition, oldPartName, partVals, oldCols, newPart);
          }
        }
      }
    } catch (NoSuchObjectException nsoe) {
      LOG.debug("Could not find db entry." + nsoe);
      //ignore
    } catch (InvalidInputException iie) {
      throw new InvalidObjectException("Invalid input to update partition column stats." + iie);
    }
  }

  private void updateTableColumnStatsForAlterTable(RawStore msdb, Table oldTable, Table newTable)
      throws MetaException, InvalidObjectException {
    String dbName = oldTable.getDbName();
    String tableName = oldTable.getTableName();
    String newDbName = HiveStringUtils.normalizeIdentifier(newTable.getDbName());
    String newTableName = HiveStringUtils.normalizeIdentifier(newTable.getTableName());

    try {
      if (!dbName.equals(newDbName) || !tableName.equals(newTableName)) {
        msdb.deleteTableColumnStatistics(dbName, tableName, null);
      } else {
        List<FieldSchema> oldCols = oldTable.getSd().getCols();
        List<FieldSchema> newCols = newTable.getSd().getCols();
        if (!MetaStoreUtils.areSameColumns(oldCols, newCols)) {
          List<String> oldColNames = new ArrayList<String>(oldCols.size());
          for (FieldSchema oldCol : oldCols) {
            oldColNames.add(oldCol.getName());
          }

          ColumnStatistics cs = msdb.getTableColumnStatistics(dbName, tableName, oldColNames);
          if (cs == null) {
            return;
          }

          List<ColumnStatisticsObj> statsObjs = cs.getStatsObj();
          for (ColumnStatisticsObj statsObj : statsObjs) {
            boolean found = false;
            for (FieldSchema newCol : newCols) {
              if (statsObj.getColName().equalsIgnoreCase(newCol.getName())
                  && statsObj.getColType().equals(newCol.getType())) {
                found = true;
                break;
              }
            }
            if (!found) {
              msdb.deleteTableColumnStatistics(dbName, tableName, statsObj.getColName());
            }
          }
        }
      }
    } catch (NoSuchObjectException nsoe) {
      LOG.debug("Could not find db entry." + nsoe);
    } catch (InvalidInputException e) {
      //should not happen since the input were verified before passed in
      throw new InvalidObjectException("Invalid inputs to update table column stats: " + e);
    }
  }
}
