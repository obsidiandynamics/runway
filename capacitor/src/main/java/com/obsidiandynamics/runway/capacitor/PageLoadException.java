package com.obsidiandynamics.runway.capacitor;

public final class PageLoadException extends PageException {
  private static final long serialVersionUID = 1L;
  
  public PageLoadException(String m) { super(m); }
  
  public PageLoadException(Throwable cause) { super(cause); }
  
  public PageLoadException(String m, Throwable cause) { super(m, cause); }
}
