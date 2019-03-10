package com.obsidiandynamics.runway.capacitor;

/**
 *  Represents any type of error that may occur while loading an object.
 */
public abstract class PageException extends Exception {
  private static final long serialVersionUID = 1L;
  
  public PageException(String m) { super(m); }
  
  public PageException(Throwable cause) { super(cause); }
  
  public PageException(String m, Throwable cause) { super(m, cause); }
}
