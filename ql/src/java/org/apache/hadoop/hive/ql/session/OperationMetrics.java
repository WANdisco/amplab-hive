package org.apache.hadoop.hive.ql.session;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics for operation ,affect row count ,for example
 */
public class OperationMetrics {
  private static final ThreadLocal<OperationMetrics>  THREAD_LOCAL_OPERATION_METRICS = new ThreadLocal(){
    @Override
    protected synchronized OperationLog initialValue() {
      return null;
    }
  };
  private long affectRowCount;

  public long getAffectRowCount() {
    return affectRowCount;
  }

  public void setAffectRowCount(long affectRowCount) {
    this.affectRowCount = affectRowCount;
  }

  public static void setCurrentOperationMetrics(OperationMetrics operationMetrics) {
    THREAD_LOCAL_OPERATION_METRICS.set(operationMetrics);
  }

  public static void removeCurrentOperationMetrics() {
    THREAD_LOCAL_OPERATION_METRICS.remove();
  }

  public static OperationMetrics getCurrentOperationMetrics() {
    return THREAD_LOCAL_OPERATION_METRICS.get();
  }
}
