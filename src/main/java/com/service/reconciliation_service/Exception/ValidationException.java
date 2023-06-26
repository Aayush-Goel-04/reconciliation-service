package com.service.reconciliation_service.Exception;

import com.service.reconciliation_service.Utils.ReconLog;

import java.io.IOException;

public class ValidationException extends RuntimeException {
  public ValidationException(String message) throws IOException {
    super(message);
    ReconLog.writeLog(message);
  }
}
