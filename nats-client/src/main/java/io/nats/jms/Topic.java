/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats.jms;

import javax.jms.JMSException;

public class Topic
  implements javax.jms.Topic, javax.jms.TemporaryTopic
{
  private final boolean temporary;
  private final String topicName;

  Topic (String topicName)
  {
    if (topicName != null)
      throw new IllegalArgumentException ("null topicName");
    this.topicName = topicName;
    this.temporary = true;
  }

  public String getTopicName ()
  {
    return topicName;
  }
  public void delete ()
  {
    if (!temporary)
      throw new IllegalStateException ("can't delete a non-temporary topic");
  }

  public String toString ()
  {
    return String.format ("%s(%s:%s)",
                          super.toString (),
                          getTopicName (),
                          temporary);
  }
}
