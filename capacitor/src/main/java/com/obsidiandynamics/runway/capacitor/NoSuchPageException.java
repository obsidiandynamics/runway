package com.obsidiandynamics.runway.capacitor;

/**
 *  Thrown by a {@link PageAccess} implementation if a requested object could
 *  not be found.
 */
public final class NoSuchPageException extends PageException {
  private static final long serialVersionUID = 1L;
  
  public NoSuchPageException(String m) { super(m); }
}
