/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats.imp;

public class Statistics
{
  public Statistics (Statistics stats)
  {
    // total hack to avoid locking elsewhere
    synchronized (stats)
    {
      this.inMsgs = stats.outMsgs;
      this.outMsgs = stats.outMsgs;
      this.inBytes = stats.inBytes;
      this.outBytes = stats.outMsgs;
      this.reconnects = stats.reconnects;
    }
  }

  public Statistics ()
  {
  }

  private long inMsgs;
  private long outMsgs;
  private long inBytes;
  private long outBytes;
  private long reconnects;
  
  public long getInMsgs () { return inMsgs; }
  public long getOutMsgs () { return outMsgs; }
  public long getInBytes () { return inBytes; }
  public long getOutBytes () { return outBytes; }
  public long getReconnects () { return reconnects; }

  // helper methods for Connection to update statistics
  synchronized void _updateBytes (int nc) {
    if (nc < 0) outBytes += -nc;
    else inBytes += nc;
  }
  synchronized void _updateMsgs (int nc) {
    if (nc < 0) outMsgs += -nc;
    else inMsgs += nc;
  }
  synchronized void _reconnectInc () {
    reconnects++;
  }

  public String toString ()
  {
    java.util.Map m = new java.util.HashMap ();
    m.put ("inMsgs", inMsgs);
    m.put ("outMsgs", outMsgs);
    m.put ("inBytes", inBytes);
    m.put ("outBytes", outBytes);
    m.put ("reconnects", reconnects);
    final String head = this.getClass ().getSimpleName ();
    final String res = (head + "{" + m.toString () + "}");
    return res;
  }
}
