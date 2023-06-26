package com.service.reconciliation_service.Exception;

import com.reconciliation.service.Utils.ReconLog;

import java.io.IOException;

public class ValidationException extends RuntimeException {
  public ValidationException(String message) throws IOException {
    super(message);
    ReconLog.writeLog(message);
  }
}
