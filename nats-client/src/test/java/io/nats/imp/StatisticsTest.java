/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats.imp;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class StatisticsTest
{
  @Test
  public void doStatsTest ()
  {
    Statistics statistics = new Statistics ();
    statistics._updateBytes (-1);
    assert (statistics.getOutBytes () == 1);
    assert (statistics.getInBytes () == 0);
    statistics._updateBytes (1);
    assert (statistics.getOutBytes () == 1);
    assert (statistics.getInBytes () == 1);


    assert (statistics.getReconnects () == 0);
    statistics._reconnectInc ();
    assert (statistics.getReconnects () == 1);

    statistics._updateMsgs (-1);
    assert (statistics.getOutMsgs () == 1);
    assert (statistics.getInMsgs () == 0);
    statistics._updateMsgs (1);
    assert (statistics.getOutMsgs () == 1);
    assert (statistics.getInMsgs () == 1);

    Statistics copy = new Statistics (statistics);
    assert (copy.getInBytes () == statistics.getInBytes ());
    assert (copy.getInMsgs () == statistics.getInMsgs ());
    assert (copy.getOutBytes () == statistics.getOutBytes ());
    assert (copy.getOutMsgs () == statistics.getOutMsgs ());
    assert (copy.getReconnects () == statistics.getReconnects ());
    assert (copy.toString ().equals (statistics.toString ()));
  }
}