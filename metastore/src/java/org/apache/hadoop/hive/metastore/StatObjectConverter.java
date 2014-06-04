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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.model.MTableColumnStatistics;

/**
 * This class contains conversion logic that creates Thrift stat objects from
 * JDO stat objects and plain arrays from DirectSQL.
 * It is hidden here so that we wouldn't have to look at it in elsewhere.
 */
public class StatObjectConverter {
  // JDO
  public static MTableColumnStatistics convertToMTableColumnStatistics(MTable table,
      ColumnStatisticsDesc statsDesc, ColumnStatisticsObj statsObj)
          throws NoSuchObjectException, MetaException, InvalidObjectException {
     if (statsObj == null || statsDesc == null) {
       throw new InvalidObjectException("Invalid column stats object");
     }

     MTableColumnStatistics mColStats = new MTableColumnStatistics();
     mColStats.setTable(table);
     mColStats.setDbName(statsDesc.getDbName());
     mColStats.setTableName(statsDesc.getTableName());
     mColStats.setLastAnalyzed(statsDesc.getLastAnalyzed());
     mColStats.setColName(statsObj.getColName());
     mColStats.setColType(statsObj.getColType());

     if (statsObj.getStatsData().isSetBooleanStats()) {
       BooleanColumnStatsData boolStats = statsObj.getStatsData().getBooleanStats();
       mColStats.setBooleanStats(boolStats.getNumTrues(), boolStats.getNumFalses(),
           boolStats.getNumNulls());
     } else if (statsObj.getStatsData().isSetLongStats()) {
       LongColumnStatsData longStats = statsObj.getStatsData().getLongStats();
       mColStats.setLongStats(longStats.getNumNulls(), longStats.getNumDVs(),
           longStats.isSetLowValue() ? longStats.getLowValue() : null,
           longStats.isSetHighValue() ? longStats.getHighValue() : null);
     } else if (statsObj.getStatsData().isSetDoubleStats()) {
       DoubleColumnStatsData doubleStats = statsObj.getStatsData().getDoubleStats();
       mColStats.setDoubleStats(doubleStats.getNumNulls(), doubleStats.getNumDVs(),
           doubleStats.isSetLowValue() ? doubleStats.getLowValue() : null,
           doubleStats.isSetHighValue() ? doubleStats.getHighValue() : null);
     } else if (statsObj.getStatsData().isSetDecimalStats()) {
       DecimalColumnStatsData decimalStats = statsObj.getStatsData().getDecimalStats();
       String low = decimalStats.isSetLowValue() ? createJdoDecimalString(decimalStats.getLowValue()) : null;
       String high = decimalStats.isSetHighValue() ? createJdoDecimalString(decimalStats.getHighValue()) : null;
       mColStats.setDecimalStats(decimalStats.getNumNulls(), decimalStats.getNumDVs(), low, high);
     } else if (statsObj.getStatsData().isSetStringStats()) {
       StringColumnStatsData stringStats = statsObj.getStatsData().getStringStats();
       mColStats.setStringStats(stringStats.getNumNulls(), stringStats.getNumDVs(),
         stringStats.getMaxColLen(), stringStats.getAvgColLen());
     } else if (statsObj.getStatsData().isSetBinaryStats()) {
       BinaryColumnStatsData binaryStats = statsObj.getStatsData().getBinaryStats();
       mColStats.setBinaryStats(binaryStats.getNumNulls(), binaryStats.getMaxColLen(),
         binaryStats.getAvgColLen());
     }
     return mColStats;
  }

  public static void setFieldsIntoOldStats(
      MTableColumnStatistics mStatsObj, MTableColumnStatistics oldStatsObj) {
    oldStatsObj.setAvgColLen(mStatsObj.getAvgColLen());
    if (mStatsObj.getLongHighValue() != null) {
      oldStatsObj.setLongHighValue(mStatsObj.getLongHighValue());
    }
    if (mStatsObj.getLongLowValue() != null) {
      oldStatsObj.setLongLowValue(mStatsObj.getLongLowValue());
    }
    if (mStatsObj.getDoubleLowValue() != null) {
      oldStatsObj.setDoubleLowValue(mStatsObj.getDoubleLowValue());
    }
    if (mStatsObj.getDoubleHighValue() != null) {
      oldStatsObj.setDoubleHighValue(mStatsObj.getDoubleHighValue());
    }
    if (mStatsObj.getDecimalLowValue() != null) {
      oldStatsObj.setDecimalLowValue(mStatsObj.getDecimalLowValue());
    }
    if (mStatsObj.getDecimalHighValue() != null) {
      oldStatsObj.setDecimalHighValue(mStatsObj.getDecimalHighValue());
    }
    oldStatsObj.setMaxColLen(mStatsObj.getMaxColLen());
    oldStatsObj.setNumDVs(mStatsObj.getNumDVs());
    oldStatsObj.setNumFalses(mStatsObj.getNumFalses());
    oldStatsObj.setNumTrues(mStatsObj.getNumTrues());
    oldStatsObj.setNumNulls(mStatsObj.getNumNulls());
    oldStatsObj.setLastAnalyzed(mStatsObj.getLastAnalyzed());
  }

  public static void setFieldsIntoOldStats(
      MPartitionColumnStatistics mStatsObj, MPartitionColumnStatistics oldStatsObj) {
    oldStatsObj.setAvgColLen(mStatsObj.getAvgColLen());
    oldStatsObj.setLongHighValue(mStatsObj.getLongHighValue());
    oldStatsObj.setDoubleHighValue(mStatsObj.getDoubleHighValue());
    oldStatsObj.setLastAnalyzed(mStatsObj.getLastAnalyzed());
    oldStatsObj.setLongLowValue(mStatsObj.getLongLowValue());
    oldStatsObj.setDoubleLowValue(mStatsObj.getDoubleLowValue());
    oldStatsObj.setDecimalLowValue(mStatsObj.getDecimalLowValue());
    oldStatsObj.setDecimalHighValue(mStatsObj.getDecimalHighValue());
    oldStatsObj.setMaxColLen(mStatsObj.getMaxColLen());
    oldStatsObj.setNumDVs(mStatsObj.getNumDVs());
    oldStatsObj.setNumFalses(mStatsObj.getNumFalses());
    oldStatsObj.setNumTrues(mStatsObj.getNumTrues());
    oldStatsObj.setNumNulls(mStatsObj.getNumNulls());
  }

  public static ColumnStatisticsObj getTableColumnStatisticsObj(
      MTableColumnStatistics mStatsObj) {
    ColumnStatisticsObj statsObj = new ColumnStatisticsObj();
    statsObj.setColType(mStatsObj.getColType());
    statsObj.setColName(mStatsObj.getColName());
    String colType = mStatsObj.getColType().toLowerCase();
    ColumnStatisticsData colStatsData = new ColumnStatisticsData();

    if (colType.equals("boolean")) {
      BooleanColumnStatsData boolStats = new BooleanColumnStatsData();
      boolStats.setNumFalses(mStatsObj.getNumFalses());
      boolStats.setNumTrues(mStatsObj.getNumTrues());
      boolStats.setNumNulls(mStatsObj.getNumNulls());
      colStatsData.setBooleanStats(boolStats);
    } else if (colType.equals("string") ||
        colType.startsWith("varchar") || colType.startsWith("char")) {
      StringColumnStatsData stringStats = new StringColumnStatsData();
      stringStats.setNumNulls(mStatsObj.getNumNulls());
      stringStats.setAvgColLen(mStatsObj.getAvgColLen());
      stringStats.setMaxColLen(mStatsObj.getMaxColLen());
      stringStats.setNumDVs(mStatsObj.getNumDVs());
      colStatsData.setStringStats(stringStats);
    } else if (colType.equals("binary")) {
      BinaryColumnStatsData binaryStats = new BinaryColumnStatsData();
      binaryStats.setNumNulls(mStatsObj.getNumNulls());
      binaryStats.setAvgColLen(mStatsObj.getAvgColLen());
      binaryStats.setMaxColLen(mStatsObj.getMaxColLen());
      colStatsData.setBinaryStats(binaryStats);
    } else if (colType.equals("bigint") || colType.equals("int") ||
        colType.equals("smallint") || colType.equals("tinyint") ||
        colType.equals("timestamp")) {
      LongColumnStatsData longStats = new LongColumnStatsData();
      longStats.setNumNulls(mStatsObj.getNumNulls());
      Long longHighValue = mStatsObj.getLongHighValue();
      if (longHighValue != null) {
        longStats.setHighValue(longHighValue);
      }
      Long longLowValue = mStatsObj.getLongLowValue();
      if (longLowValue != null) {
        longStats.setLowValue(longLowValue);
      }
      longStats.setNumDVs(mStatsObj.getNumDVs());
      colStatsData.setLongStats(longStats);
    } else if (colType.equals("double") || colType.equals("float")) {
      DoubleColumnStatsData doubleStats = new DoubleColumnStatsData();
      doubleStats.setNumNulls(mStatsObj.getNumNulls());
      Double doubleHighValue = mStatsObj.getDoubleHighValue();
      if (doubleHighValue != null) {
        doubleStats.setHighValue(doubleHighValue);
      }
      Double doubleLowValue = mStatsObj.getDoubleLowValue();
      if (doubleLowValue != null) {
        doubleStats.setLowValue(doubleLowValue);
      }
      doubleStats.setNumDVs(mStatsObj.getNumDVs());
      colStatsData.setDoubleStats(doubleStats);
    } else if (colType.equals("decimal")) {
      DecimalColumnStatsData decimalStats = new DecimalColumnStatsData();
      decimalStats.setNumNulls(mStatsObj.getNumNulls());
      String decimalHighValue = mStatsObj.getDecimalHighValue();
      if (decimalHighValue != null) {
        decimalStats.setHighValue(createThriftDecimal(decimalHighValue));
      }
      String decimalLowValue = mStatsObj.getDecimalLowValue();
      if (decimalLowValue != null) {
        decimalStats.setLowValue(createThriftDecimal(decimalLowValue));
      }
      decimalStats.setNumDVs(mStatsObj.getNumDVs());
      colStatsData.setDecimalStats(decimalStats);
    }
    statsObj.setStatsData(colStatsData);
    return statsObj;
  }

  public static ColumnStatisticsDesc getTableColumnStatisticsDesc(
      MTableColumnStatistics mStatsObj) {
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc();
    statsDesc.setIsTblLevel(true);
    statsDesc.setDbName(mStatsObj.getDbName());
    statsDesc.setTableName(mStatsObj.getTableName());
    statsDesc.setLastAnalyzed(mStatsObj.getLastAnalyzed());
    return statsDesc;
  }

  public static MPartitionColumnStatistics convertToMPartitionColumnStatistics(
      MPartition partition, ColumnStatisticsDesc statsDesc, ColumnStatisticsObj statsObj)
          throws MetaException, NoSuchObjectException {
    if (statsDesc == null || statsObj == null) {
      return null;
    }

    MPartitionColumnStatistics mColStats = new MPartitionColumnStatistics();
    mColStats.setPartition(partition);
    mColStats.setDbName(statsDesc.getDbName());
    mColStats.setTableName(statsDesc.getTableName());
    mColStats.setPartitionName(statsDesc.getPartName());
    mColStats.setLastAnalyzed(statsDesc.getLastAnalyzed());
    mColStats.setColName(statsObj.getColName());
    mColStats.setColType(statsObj.getColType());

    if (statsObj.getStatsData().isSetBooleanStats()) {
      BooleanColumnStatsData boolStats = statsObj.getStatsData().getBooleanStats();
      mColStats.setBooleanStats(boolStats.getNumTrues(), boolStats.getNumFalses(),
          boolStats.getNumNulls());
    } else if (statsObj.getStatsData().isSetLongStats()) {
      LongColumnStatsData longStats = statsObj.getStatsData().getLongStats();
      mColStats.setLongStats(longStats.getNumNulls(), longStats.getNumDVs(),
          longStats.isSetLowValue() ? longStats.getLowValue() : null,
          longStats.isSetHighValue() ? longStats.getHighValue() : null);
    } else if (statsObj.getStatsData().isSetDoubleStats()) {
      DoubleColumnStatsData doubleStats = statsObj.getStatsData().getDoubleStats();
      mColStats.setDoubleStats(doubleStats.getNumNulls(), doubleStats.getNumDVs(),
          doubleStats.isSetLowValue() ? doubleStats.getLowValue() : null,
          doubleStats.isSetHighValue() ? doubleStats.getHighValue() : null);
    } else if (statsObj.getStatsData().isSetDecimalStats()) {
      DecimalColumnStatsData decimalStats = statsObj.getStatsData().getDecimalStats();
      String low = decimalStats.isSetLowValue() ? createJdoDecimalString(decimalStats.getLowValue()) : null;
      String high = decimalStats.isSetHighValue() ? createJdoDecimalString(decimalStats.getHighValue()) : null;
      mColStats.setDecimalStats(decimalStats.getNumNulls(), decimalStats.getNumDVs(), low, high);
    } else if (statsObj.getStatsData().isSetStringStats()) {
      StringColumnStatsData stringStats = statsObj.getStatsData().getStringStats();
      mColStats.setStringStats(stringStats.getNumNulls(), stringStats.getNumDVs(),
        stringStats.getMaxColLen(), stringStats.getAvgColLen());
    } else if (statsObj.getStatsData().isSetBinaryStats()) {
      BinaryColumnStatsData binaryStats = statsObj.getStatsData().getBinaryStats();
      mColStats.setBinaryStats(binaryStats.getNumNulls(), binaryStats.getMaxColLen(),
        binaryStats.getAvgColLen());
    }
    return mColStats;
  }

  public static ColumnStatisticsObj getPartitionColumnStatisticsObj(
      MPartitionColumnStatistics mStatsObj) {
    ColumnStatisticsObj statsObj = new ColumnStatisticsObj();
    statsObj.setColType(mStatsObj.getColType());
    statsObj.setColName(mStatsObj.getColName());
    String colType = mStatsObj.getColType().toLowerCase();
    ColumnStatisticsData colStatsData = new ColumnStatisticsData();

    if (colType.equals("boolean")) {
      BooleanColumnStatsData boolStats = new BooleanColumnStatsData();
      boolStats.setNumFalses(mStatsObj.getNumFalses());
      boolStats.setNumTrues(mStatsObj.getNumTrues());
      boolStats.setNumNulls(mStatsObj.getNumNulls());
      colStatsData.setBooleanStats(boolStats);
    } else if (colType.equals("string") ||
        colType.startsWith("varchar") || colType.startsWith("char")) {
      StringColumnStatsData stringStats = new StringColumnStatsData();
      stringStats.setNumNulls(mStatsObj.getNumNulls());
      stringStats.setAvgColLen(mStatsObj.getAvgColLen());
      stringStats.setMaxColLen(mStatsObj.getMaxColLen());
      stringStats.setNumDVs(mStatsObj.getNumDVs());
      colStatsData.setStringStats(stringStats);
    } else if (colType.equals("binary")) {
      BinaryColumnStatsData binaryStats = new BinaryColumnStatsData();
      binaryStats.setNumNulls(mStatsObj.getNumNulls());
      binaryStats.setAvgColLen(mStatsObj.getAvgColLen());
      binaryStats.setMaxColLen(mStatsObj.getMaxColLen());
      colStatsData.setBinaryStats(binaryStats);
    } else if (colType.equals("tinyint") || colType.equals("smallint") ||
        colType.equals("int") || colType.equals("bigint") ||
        colType.equals("timestamp")) {
      LongColumnStatsData longStats = new LongColumnStatsData();
      longStats.setNumNulls(mStatsObj.getNumNulls());
      if (mStatsObj.getLongHighValue() != null) {
        longStats.setHighValue(mStatsObj.getLongHighValue());
      }
      if (mStatsObj.getLongLowValue() != null) {
        longStats.setLowValue(mStatsObj.getLongLowValue());
      }
      longStats.setNumDVs(mStatsObj.getNumDVs());
      colStatsData.setLongStats(longStats);
    } else if (colType.equals("double") || colType.equals("float")) {
      DoubleColumnStatsData doubleStats = new DoubleColumnStatsData();
      doubleStats.setNumNulls(mStatsObj.getNumNulls());
      if (mStatsObj.getDoubleHighValue() != null) {
        doubleStats.setHighValue(mStatsObj.getDoubleHighValue());
      }
      if (mStatsObj.getDoubleLowValue() != null) {
        doubleStats.setLowValue(mStatsObj.getDoubleLowValue());
      }
      doubleStats.setNumDVs(mStatsObj.getNumDVs());
      colStatsData.setDoubleStats(doubleStats);
    } else if (colType.equals("decimal")) {
      DecimalColumnStatsData decimalStats = new DecimalColumnStatsData();
      decimalStats.setNumNulls(mStatsObj.getNumNulls());
      if (mStatsObj.getDecimalHighValue() != null) {
        decimalStats.setHighValue(createThriftDecimal(mStatsObj.getDecimalHighValue()));
      }
      if (mStatsObj.getDecimalLowValue() != null) {
        decimalStats.setLowValue(createThriftDecimal(mStatsObj.getDecimalLowValue()));
      }
      decimalStats.setNumDVs(mStatsObj.getNumDVs());
      colStatsData.setDecimalStats(decimalStats);
    }
    statsObj.setStatsData(colStatsData);
    return statsObj;
  }

  public static ColumnStatisticsDesc getPartitionColumnStatisticsDesc(
    MPartitionColumnStatistics mStatsObj) {
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc();
    statsDesc.setIsTblLevel(false);
    statsDesc.setDbName(mStatsObj.getDbName());
    statsDesc.setTableName(mStatsObj.getTableName());
    statsDesc.setPartName(mStatsObj.getPartitionName());
    statsDesc.setLastAnalyzed(mStatsObj.getLastAnalyzed());
    return statsDesc;
  }

  // SQL
  public static void fillColumnStatisticsData(String colType, ColumnStatisticsData data,
      Object llow, Object lhigh, Object dlow, Object dhigh, Object declow, Object dechigh,
      Object nulls, Object dist, Object avglen, Object maxlen, Object trues, Object falses) {
    if (colType.equals("boolean")) {
      BooleanColumnStatsData boolStats = new BooleanColumnStatsData();
      boolStats.setNumFalses((Long)falses);
      boolStats.setNumTrues((Long)trues);
      boolStats.setNumNulls((Long)nulls);
      data.setBooleanStats(boolStats);
    } else if (colType.equals("string") ||
        colType.startsWith("varchar") || colType.startsWith("char")) {
      StringColumnStatsData stringStats = new StringColumnStatsData();
      stringStats.setNumNulls((Long)nulls);
      stringStats.setAvgColLen((Double)avglen);
      stringStats.setMaxColLen((Long)maxlen);
      stringStats.setNumDVs((Long)dist);
      data.setStringStats(stringStats);
    } else if (colType.equals("binary")) {
      BinaryColumnStatsData binaryStats = new BinaryColumnStatsData();
      binaryStats.setNumNulls((Long)nulls);
      binaryStats.setAvgColLen((Double)avglen);
      binaryStats.setMaxColLen((Long)maxlen);
      data.setBinaryStats(binaryStats);
    } else if (colType.equals("bigint") || colType.equals("int") ||
        colType.equals("smallint") || colType.equals("tinyint") ||
        colType.equals("timestamp")) {
      LongColumnStatsData longStats = new LongColumnStatsData();
      longStats.setNumNulls((Long)nulls);
      if (lhigh != null) {
        longStats.setHighValue((Long)lhigh);
      }
      if (llow != null) {
        longStats.setLowValue((Long)llow);
      }
      longStats.setNumDVs((Long)dist);
      data.setLongStats(longStats);
    } else if (colType.equals("double") || colType.equals("float")) {
      DoubleColumnStatsData doubleStats = new DoubleColumnStatsData();
      doubleStats.setNumNulls((Long)nulls);
      if (dhigh != null) {
        doubleStats.setHighValue((Double)dhigh);
      }
      if (dlow != null) {
        doubleStats.setLowValue((Double)dlow);
      }
      doubleStats.setNumDVs((Long)dist);
      data.setDoubleStats(doubleStats);
    } else if (colType.equals("decimal")) {
      DecimalColumnStatsData decimalStats = new DecimalColumnStatsData();
      decimalStats.setNumNulls((Long)nulls);
      if (dechigh != null) {
        decimalStats.setHighValue(createThriftDecimal((String)dechigh));
      }
      if (declow != null) {
        decimalStats.setLowValue(createThriftDecimal((String)declow));
      }
      decimalStats.setNumDVs((Long)dist);
      data.setDecimalStats(decimalStats);
    }
  }

  private static Decimal createThriftDecimal(String s) {
    BigDecimal d = new BigDecimal(s);
    return new Decimal(ByteBuffer.wrap(d.unscaledValue().toByteArray()), (short)d.scale());
  }

  private static String createJdoDecimalString(Decimal d) {
    return new BigDecimal(new BigInteger(d.getUnscaled()), d.getScale()).toString();
  }
}
