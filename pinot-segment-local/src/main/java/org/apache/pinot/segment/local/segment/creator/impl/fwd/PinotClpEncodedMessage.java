package org.apache.pinot.segment.local.segment.creator.impl.fwd;

public class PinotClpEncodedMessage {
  private String _logType;
  private String[] _dictionaryVars;
  private long[] _encodedVars;

  public PinotClpEncodedMessage() {
  }

  public String getLogType() {
    return _logType;
  }

  public String[] getDictionaryVars() {
    return _dictionaryVars;
  }

  public long[] getEncodedVars() {
    return _encodedVars;
  }

  public PinotClpEncodedMessage setLogType(String logType) {
    _logType = logType;
    return this;
  }

  public PinotClpEncodedMessage setDictionaryVars(String[] dictionaryVars) {
    _dictionaryVars = dictionaryVars;
    return this;
  }

  public PinotClpEncodedMessage setEncodedVars(long[] encodedVars) {
    _encodedVars = encodedVars;
    return this;
  }
}
