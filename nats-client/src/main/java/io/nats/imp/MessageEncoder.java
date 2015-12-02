/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats.imp;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class MessageEncoder
  extends MessageToByteEncoder<Message>
{
  @Override
  public void encode (ChannelHandlerContext ctx,
                      Message msg,
                      ByteBuf out)
  throws Exception
  {
    final int wi_b = out.writerIndex ();
    msg.encode (out);
    final int wi_a = out.writerIndex ();
    final int nw = wi_a - wi_b;
    assert (nw != 0);
  }
}
