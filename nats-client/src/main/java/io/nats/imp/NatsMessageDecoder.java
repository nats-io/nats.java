/*
 * Copyright 2015 Apcera Inc. All rights reserved.
 */

package io.nats.imp;

import io.nats.NatsException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.CharsetUtil;

import java.util.List;

/**
 * Decode bytes into NATS message instances
 *
 * See nats protocol docs and sources for details ;-)
 *
 * What color is YOUR dragon??? I like red... There are other versions
 * of this that use a more academic approach, but this times out and profiles
 * nicely for now. It creates little if any memory except where required by Java
 * and there are some ways around that if we trade space for time.
 *
 * Specifically, this version can be further optimized to:
 * <ul>
 *   <ii>Not create instances of java.lang.String. Either use a lookup with wrapper to avoid creating the
 *   same strings over and over or some on the fly hash technique. This comment applies to Subject and reply
 *   queue names. We have optimized the sid per dlc's pattern around using longs.
 *   </ii>
 *   <li>We could decode the message body into whatever type the user wants on the fly in the same thread
 *   without copying bytes/decoding again seperately. Look at jams' perforce factors branch, bring it over
 *   once API stabalizes.
 *   </li>
  * </ul>
 * The current version optimizes touching of bytes in the incoming stream
 * it's assumed we have an <i>unbounded</i> byte buffer as input. This decodes
 * raw bytes into instances of NATS Messages.
 *
 * FIXME: jam: Need a netty and jvm/heap profiing expert to pick this apart. Please do if you are!! My eyes are
 * to red and blurry to see any further nits.
 *
 * @see ByteToMessageDecoder
 */
class NatsMessageDecoder extends ByteToMessageDecoder
{
  /** The protocol doc and code are not in sync with regard to EOL. We can handle both, by
   * default we are strict.
   * FIXME: jam: talk to lm/dlc about protocol issues and merge in analysis docs.
   */
  static private final boolean STRICT_PARSING_PER_SPEC = true;

  /** The max frame length we are willing to decode (parse). */
  private final int maxFrameLength;
  /** Should we fail earlier rather than later ? */
  private final boolean failFast;
  /** Map used when parsing json */
  private java.util.Map<String,String> json = new java.util.HashMap<String,String> ();

  /** Create a new decoder with the given maximum frame length
   *
   * @param maxFrameLength the maximum frame length this decoder will decode. A NatsException will be
   * raised if this decoder tries to decode any message exceeding this length.
   * @param failFast true if this decoder should fail sooner rather than later. By default this decoder
   * requires a four byte look ahead unless <i>failFast</i> is true
   */
  public NatsMessageDecoder (int maxFrameLength, boolean failFast)
  {
    this.maxFrameLength = maxFrameLength;
    this.failFast = failFast;
    this.setSingleDecode (false);
    reset_state ();
  }

  protected final void decode (ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
    throws Exception
  {
    final int old_ri = in.readerIndex ();
    Object decoded = decode (ctx, in);
    final int new_ri = in.readerIndex ();
    final int nread = new_ri - old_ri;
    if (decoded != null)
      {
        out.add (decoded);
      }
  }

  private void fail (final ChannelHandlerContext ctx, String why)
  {
    _saved_state = FINAL_ERROR;
    ctx.fireExceptionCaught (new NatsException (why));
  }


  // there is a method to the madness...
  // touch this and perhaps blow everything up
  // we have a couple implementation goals to try and adhere to:
  // 1. Minimize per-message processing space usage at ALL COSTS
  // 2. Minimize paging / cache / other issues by touching bytes more than once
  // 3. Minimize compute where we can
  // so, we could use an enum but java sucks in this regard

  static private final int VERB_NONE = -1, VERB_INFO = 0,
    VERB_CONNECT = 1, VERB_PUB = 2,
    VERB_SUB = 3, VERB_UNSUB = 4, VERB_MSG = 5,
    VERB_PING = 6, VERB_PONG = 7, VERB_OK = 8, VERB_ERR = 9;

  static private final int START = 0,
    HEADER_FINAL_R = 1,
    HEADER_FINAL = 2,
    FINAL_ERROR = 3,
    JSON_OBJ = 4, JSON_MAYBE_VAL = 5,  JSON_I = 6, JSON_K = 7,
    JSON_SEP = 8, JSON_SEPE = 9, JSON_V = 10, JSON_VE = 11,
    CONNECT_VERB_E = 13,
    CONNECT_VERB_C = 14,
    CONNECT_VERB_T = 15,
    UNSU_VERB_B = CONNECT_VERB_T + 1,
    CONNECT_INFO_ARG = UNSU_VERB_B + 1,
    CONNECT_INFO_ARG_REST = CONNECT_INFO_ARG + 1,
    ERR_MESG = CONNECT_INFO_ARG_REST + 1,
  ERR_MESG_READ = ERR_MESG + 1,
  MSG_BODY = ERR_MESG_READ + 1,
  ARG = MSG_BODY + 1, ARG_1 = ARG + 1, ARG_2 = ARG_1 + 1, ARG_3 = ARG_2 + 1, ARG_4 = ARG_3 + 1, ARG_DONE = ARG_4 + 1;

  // the parser is a simple stack parser that accumulates up to N states
  // as we parse things and get into a message new states may be parsed
  // state_sp is our stack pointer ala x86 ;-)
  // FIXME: jam: invariant assertions between states, state_sp and state
  private int states[] = new int[4];
  private int state_sp = 0;
  private int _saved_state = START;
  private int verb = VERB_NONE;

  /** push a new state onto the stack */
  private int push (ChannelHandlerContext ctx, int cs, int ns)
  {
    if (state_sp >= states.length) // defensive, should only be ==, but...
      {
        // NOTE: this modifies state_sp, states, etc... to mark this decoider
        // as in FINAL_ERROR state.
        fail (ctx, "max parser state reached: sp=" + state_sp + " cs=" + cs + " ns=" + ns);
        return FINAL_ERROR;
      }
    // save the current state.
    states[state_sp++] = cs;
    return ns;
  }

  /** pop the current state and put the parser into the underlying state on the stack */
  private int pop (ChannelHandlerContext ctx)
  {
    int res;

    if (state_sp <= 0)  // defensive, should only be ==, but ...
    {
      fail (ctx, "invalid parser state stack");
      return FINAL_ERROR;
    }
    res = states[--state_sp];
    return res;
  }

  /** Is b equal to left or right? */
  private int or (int b, int left, int right, int new_state)
  {
    return (b == left || b == right) ? new_state : FINAL_ERROR;
  }


  /** Try to optimize looking at bytes.
   *
   * I'd like to do something like the equivalent in C but Java complains
   *   static final private int FOO = word ('I', 'N', 'F', 'O');
   *
   */
  static final private int WORD_INFO = 'I' << 24 | 'N' << 16 | 'F' << 8 | 'O';
  static final private int WORD_CONN = 'C' << 24 | 'O' << 16 | 'N' << 8 | 'N';
  static final private int WORD_PUB = 'P' << 24 | 'U' << 16 | 'B' << 8;
  static final private int WORD_SUB = 'S' << 24 | 'U' << 16 | 'B' << 8;
  static final private int WORD_UNSU = 'U' << 24 | 'N' << 16 | 'S' << 8 | 'U';
  static final private int WORD_MSG = 'M' << 24 | 'S' << 16 | 'G' << 8;
  static final private int WORD_PING = 'P' << 24 | 'I' << 16 | 'N' << 8 | 'G';
  static final private int WORD_PONG = 'P' << 24 | 'O' << 16 | 'N' << 8 | 'G';
  static final private int WORD_OK = '+' << 24 | 'O' << 16 | 'K' << 8;
  static final private int WORD_ERR = '-' << 24 | 'E' << 16 | 'R' << 8 | 'R';

  static boolean is_message_starter_p (ByteBuf buf, int b, int ri, int wi)
  {
    // FIXME: jam: failfast support only checks first bytes here, we need four bytes... fill this in
    switch (upper (b))
    {
    case 'I':
    case 'C':
    case 'P':
    case 'S':
    case 'U':
    case 'M':
    case '+':
    case '-':
      return true;
    }
    return false;
  }
  private int read_verb_start (final int b1, final int b2,
                               final int b3, final int b4)
  {
    final int w = b1 << 24 | b2 << 16 | b3 << 8 | b4;
    switch (w)
    {
    case WORD_INFO: this.verb = VERB_INFO; return CONNECT_INFO_ARG;
    case WORD_CONN: this.verb = VERB_CONNECT; return CONNECT_VERB_E;
    case WORD_PUB + '\t':
    case WORD_PUB + ' ': this.verb = VERB_PUB; return ARG_1;
    case WORD_SUB + '\t':
    case WORD_SUB + ' ': this.verb = VERB_SUB; return ARG_1;
    case WORD_UNSU: this.verb = VERB_UNSUB; return UNSU_VERB_B;
    case WORD_MSG + '\t':
    case WORD_MSG + ' ': this.verb = VERB_MSG; return ARG_1;
    case WORD_PING: this.verb = VERB_PING; return HEADER_FINAL;
    case WORD_PONG: this.verb = VERB_PONG; return HEADER_FINAL;
    case WORD_OK + '\r': this.verb = VERB_OK; return HEADER_FINAL_R;
    case WORD_ERR: this.verb = VERB_ERR; return ERR_MESG;
    default: this.verb = VERB_NONE; return FINAL_ERROR;
    }
  }

  private static int upper (int b)
  {
    return (b >= 'a' && b <= 'z') ? ('A' + (b - 'a')) : b;
  }

  private boolean arg_is_all_digits = false; // we only saw digits and were able to convert them?
  private long arg_digit_value = 0; // the value of the argument as long, assuming arg
  private DestinationImpl argv_sub;          // subject read from the message
  private SubscriberKey argv_sid;          // optmized sid from the message (e.g., long)
  private long argv_body_size;      // body size from the message
  private DestinationImpl argv_reply_or_queue; // duh
  private ByteBuf msg_body;         // where we are reading the body into
  private int msg_body_size;        // and its size (including trailing \r\n)
  private String json_arg;          // pushed json argument
  private int argc;                 // args to current verb
  private ByteBuf arg_buf;          // the arg we're collecting's buffer (pointer to array below)
  private final ByteBuf argv[] = {  // argument vectors for the verb, arg_buf points to one based upon argc
    ByteBufAllocator.DEFAULT.heapBuffer (),
    ByteBufAllocator.DEFAULT.heapBuffer (),
    ByteBufAllocator.DEFAULT.heapBuffer (),
    ByteBufAllocator.DEFAULT.heapBuffer ()
  };
  // indicates whether we have a number if not -1, what a hack
  // FIXME: jam: get real man
  private final long argv_as_number[] = new long[argv.length];

  private boolean
  finish_arg (ChannelHandlerContext ctx)
  {
    assert (argc < argv.length);

    if (arg_buf.isReadable () == false)
      {
        fail (ctx, "no chars in verb: " + verb + " argv[" + argc + "]");
        arg_is_all_digits = false;
        argv_as_number[argc] = -1;
        return false;
      }

    if (arg_is_all_digits)
      argv_as_number[argc] = arg_digit_value;
    else
      argv_as_number[argc] = -1;

    arg_is_all_digits = true;
    arg_digit_value = 0;
    arg_buf = (++argc < argv.length ? argv[argc] : null);
    if (arg_buf != null)
      arg_buf.clear ();
    return true;
  }

  private boolean
  finish_arg_state (ChannelHandlerContext ctx)
  {
    String why = null;

    argv_sid = null;
    argv_sub = null;
    argv_body_size = -1;
    argv_reply_or_queue = null;

    // see if we have the right number of arguments for the given
    // verb
    switch (this.verb << 4 | argc)
    {
    case VERB_MSG << 4 | 4:
      argv_reply_or_queue = destination (argv[2]);
      // fall thru..
    case VERB_MSG << 4 | 3:
      {
        assert (argc >= 3);
        if (argv_as_number[argc-1] < 0)
          {
            why = ("Invalid MSG length: " +
                   argv[argc-1].toString (CharsetUtil.US_ASCII));
            break;
          }
        argv_body_size = argv_as_number[argc-1];
        assert (argv_body_size == new Long (argv[argc-1].toString (CharsetUtil.US_ASCII)).intValue ());
        argv_sub = destination (argv[0]);
        argv_sid = (argv_as_number[1] == -1
                    ? SubscriberKey.newInstance (argv[1])
                    : SubscriberKey.newInstance (argv_as_number[1]));
      }
      break;
    case VERB_PUB << 4 | 3:
      argv_reply_or_queue = destination (argv[1]);
      // fall thru
    case VERB_PUB << 4 | 2:
      {
        if (argv_as_number[argc-1] < 0)
        {
          why = ("Invalid PUB length: " +
                 argv[argc-1].toString (CharsetUtil.US_ASCII));
          break;
        }

        argv_body_size = argv_as_number[argc-1];
        assert (argv_body_size
                == new Long (argv[argc-1].toString (CharsetUtil.US_ASCII)).intValue ());
        argv_sub = destination (argv[0]);
        break;
      }
    case VERB_SUB << 4 | 3:
      argv_reply_or_queue = destination (argv[1]);
      // fall thru...
    case VERB_SUB << 4 | 2:
    case VERB_UNSUB << 4 | 2:
      {
        argv_sub = destination (argv[0]);
        argv_sid = (argv_as_number[argc-1] == -1
                    ? SubscriberKey.newInstance (argv[argc-1])
                    : SubscriberKey.newInstance (argv_as_number[argc-1]));
        break;
      }
    default:
      why = ("Invalid arguments to verb=" + this.verb
             + " argc=" + argc);
    }
    if (why != null)
      fail (ctx, why);
    return why == null;
  }

  // reset the decoder's state after reading a message or upon startup
  private void
  reset_state ()
  {
    arg_digit_value = 0;
    argv_as_number[0] = argv_as_number[1] = argv_as_number[2] = argv_as_number[3] = -1;

    arg_is_all_digits = true;
    state_sp = 0;
    _saved_state = START;
    verb = VERB_NONE;

    msg_body = null;
    msg_body_size = -1;

    argc = 0;
    arg_buf = argv[0];
    arg_buf.clear ();
    json_arg = null;
  }

  DestinationImpl
  destination (ByteBuf buf)
  {
    final String asString = buf.toString (CharsetUtil.US_ASCII);
    return new DestinationImpl (asString);
  }
  /**
   * Decodes the body of the current message (aka a MSG/PUB body) into msg_body
   * returns null and will continue decoding when more data is required.
   */
  protected Message
  decode_body (ChannelHandlerContext ctx, ByteBuf buff, int ri, int wi)
  {
    assert (msg_body_size > 0);

    final int nleft = wi - ri;
    assert (nleft >= 0);
    if (nleft == 0) // no data left to read from source
      return null;

    final int want = msg_body_size;

    final int to_copy = want <= nleft ? want : nleft;
    assert (to_copy > 0);
    assert (ri + to_copy <= wi);
    assert (to_copy <= want);
    assert (to_copy <= nleft);
    // copy to_copy bytes into the body
    msg_body.writeBytes (buff, ri, to_copy);
    // update the soruces reader index
    buff.readerIndex (ri + to_copy);
    // and our remaining size...
    msg_body_size -= to_copy;
    // are we there yet?
    if (msg_body_size == 0)
      {
        final int eom = msg_body.writerIndex ();

        // do we match a trailing \r\n?
        assert (eom >= 2);
        int b1 = msg_body.getUnsignedByte (eom - 2);
        int b2 = msg_body.getUnsignedByte (eom - 1);
        if (b1 != '\r' || b2 != '\n')
          {
            // FIXME: dlc code does not appear to require \r\n, only trailing two bytes
            fail (ctx, "Missing trailing '\\r\\n' in MSG (eom=" + eom + ") " + (b1 + "") + "/" + b2);
            return null;
          }
        // chop off the \r\n
        msg_body.writerIndex (eom - 2);
        // reset the reader index so the receiver gets the whole thing
        msg_body.readerIndex (0);
        // we are done!!
        final Message res = (verb == VERB_MSG
          ? new Message.MSG (argv_sub, argv_sid, argv_reply_or_queue, msg_body)
          : new Message.PUB (argv_sub, argv_reply_or_queue, msg_body));
        return res;
      }
    assert (msg_body_size > 0);
    // still have more to read...
    return null;
  }

  /** Decodes one message from this channel, return null if we need more data */
  protected Message
  decode (ChannelHandlerContext ctx, ByteBuf buffer)
    throws Exception
  {
    // how much more do we have to read?
    final int wi = buffer.writerIndex ();
    // keep a local copy...
    int cur_state = _saved_state;
    // where are we in the buffer?
    int ri = buffer.readerIndex ();

    // if we're in the middle of a body then continue...
    if (cur_state == MSG_BODY)
      {
        final Message body = decode_body (ctx, buffer, ri, wi);
        if (body != null)
          reset_state ();
        return body;
      }

    try
      {
        for (; ri < wi; ri++)
          {
            final int b = buffer.getUnsignedByte (ri);
            if (STRICT_PARSING_PER_SPEC && cur_state == START)
              {
                if (b == ' ' || b == '\t' || b == '\r' || b == '\n') continue;
              }
            switch (cur_state)
             {
             case START:
               {
                 if (STRICT_PARSING_PER_SPEC
                     && (b == ' ' || b == '\t' || b == '\r' || b == '\n'))
                   continue;
                 // see if we have at least four bytes, if not, exit out now...
                 if (wi - ri >= 4)
                  {
                    final int b2 = buffer.getUnsignedByte (ri + 1);
                    final int b3 = buffer.getUnsignedByte (ri + 2);
                    final int b4 = buffer.getUnsignedByte (ri + 3);
                    cur_state = read_verb_start (b, b2, b3, b4);
                    if (cur_state == FINAL_ERROR)
                      {
                        // We are stupid and try all upper case first, assuming
                        // that is the "normal case"., Per the spec we need to
                        // support mixed case. Try again brute force, upper will
                        // only upper case letters that need to be.
                        cur_state = read_verb_start (upper (b), upper (b2),
                                                     upper (b3), upper (b4));
                        if (cur_state == FINAL_ERROR)
                          {
                            break;
                          }
                      }
                    ri += 3;
                    // we matched a verb, continue the loop
                    assert (cur_state != START);
                    continue;
                  }
                 if (failFast && ! is_message_starter_p (buffer, b, ri, wi))
                  break; // we don't match what is there positively...
                 // I'll be back...
                 // When there's at least four bytes...
                 return null;
               }
             // We've read a verb and are sitting on a \r
             case HEADER_FINAL_R:
               if (b == ' ' || b == '\t') { cur_state = HEADER_FINAL; continue; }
               if (b == '\r') continue;
               if (b == '\n') // we're done with the verb line
                 {
                   ri++;  // eat it
                   Message res;

                   // Take us home toto...
                   switch (verb)
                   {
                   case VERB_OK: res = Message.OK; break;
                   case VERB_PING: res = Message.PING; break;
                   case VERB_PONG: res = Message.PONG; break;
                   case VERB_CONNECT: res = Message.CONNECT (json); break;
                   case VERB_INFO: res = Message.INFO (json); break;
                   case VERB_ERR:
                     res = new Message.ERR (arg_buf.toString (CharsetUtil.US_ASCII));
                     break;
                   case VERB_SUB:
                     res = new  Message.SUB (argv_sub, argv_sid,
                                             argv_reply_or_queue);
                     break;
                   case VERB_UNSUB:
                     res = new Message.UNSUB (argv_sub, 0); // JAM FIXME: max argv_sid.value ());
                     break;
                   case VERB_MSG: case VERB_PUB:
                     {
                       msg_body_size = ((int) argv_body_size);
                       assert (msg_body_size == argv_body_size);
                       // should keep track of total frame size... done higher
                       // level currently...
                       if (msg_body_size >= maxFrameLength || msg_body_size < 0)
                       {
                         fail (ctx, "invalid body length: " + msg_body_size
                                    + " (max " + maxFrameLength + ")");
                         return null;
                       }
                       // + \r\n
                       msg_body_size += 2;
                       // if the client handler could decode on the fly here
                       // we could avoid a copy...
                       msg_body = ByteBufAllocator.DEFAULT.heapBuffer (msg_body_size);
                       msg_body.clear ();

                       // we need to read the body now
                       // uncache right *before calling decode body*
                       _saved_state = MSG_BODY;
                       buffer.readerIndex (ri);

                       res = decode_body (ctx, buffer, ri, wi);
                       // we're inside a finally that resets our cached ivars :-(
                       // re-cache things right *after calling decode body*...
                       ri = buffer.readerIndex ();
                       cur_state = _saved_state;
                     }
                    break;
                   default:
                     throw new IllegalStateException ("unknown type: " + verb);
                   }

                   if (res != null)
                    {
                      reset_state ();
                      cur_state = _saved_state;
                    }
                   else
                    {
                      // we have an error? are in a bad state
                      // check invariants
                      // FIXME: jam: we have an issue here, need
                      // to break when not in MSG_BODY any more
                    }
                   return res;
                 }
               break;
             // we are expecting the end of the header
             case HEADER_FINAL:
               switch (b)
               {
               case '\n': // dlc allows \n without \r\n
                 if (STRICT_PARSING_PER_SPEC) break;
                 ri--;
                 // treat it like a \r and fall thru to HEADER_FINAL_R
               case '\r':
                 cur_state = HEADER_FINAL_R;
                 continue;
               case ' ': case '\t':
                 continue;
               }
               break;
             // we are starting an argument
             // 1...4 and back here when done.
             case ARG_DONE:
             case ARG_4:
               // only MSG
             case ARG_3:
               // only MSG, PUB
             case ARG_1:
             case ARG_2:
               // we have at least a verb and trailing space(s)
               // we are reading an argument, ARG_1...ARG_4 depending
               // on the value of argc...
               assert (argc == (cur_state - ARG_1)); // ensure that's the case...

               // Trim leading white space
               if (b == ' ' || b == '\t')
                 continue;

               if (b == '\r' || b == '\n')
                 {
                   // we are at thee end of all arguments...
                   // wrap up and validate we have enough
                   // arguments, their semantics, etc...
                   if (! finish_arg_state (ctx))
                    {
                      break;
                    }
                   // we need to back up our reader index
                   // we consumed the trailing newline or
                   // carriage return...
                   cur_state = (b == '\r' ? HEADER_FINAL_R : HEADER_FINAL);
                   ri--;
                   continue;
                 }

               // we have an argument.
               // make sure we do not have to many args...
               if (cur_state == ARG_DONE)
                {
                  fail (ctx, "to many arguments: " + argc);
                  break;
                }
               cur_state = push (ctx, cur_state, ARG);
               // fall thru here to read the first
               // argument char
             case ARG:
               switch (b)
               {
               case ' ': case '\t': case '\r': case '\n':
                 // back up one, our argument is over
                 // go back to the previous state.
                 ri--;
                 // save the current argument...
                 if (! finish_arg (ctx))
                   break;
                 cur_state = pop (ctx);
                 cur_state++;
                 continue;
               default:
                 arg_is_all_digits = false;
                 // fall thru...
               case '0': case '1': case '2': case '3': case '4':
               case '5': case '6': case '7': case '8': case '9':
                 // watch out from above
                 if (arg_is_all_digits)
                  {
                    long chk = arg_digit_value;

                    arg_digit_value *= 10;
                    arg_digit_value += (b - '0');
                    assert (chk == arg_digit_value / 10);
                    chk = arg_digit_value % 10;
                    assert (chk == b - '0');
                  }
                arg_buf.writeByte (b);
                continue;
               }
             case ERR_MESG:
               if (b == ' ' || b == '\t') continue;
               // Set this up in case
               // We have an empty error message
               arg_buf = argv[0];
               arg_buf.clear ();
               cur_state = ERR_MESG_READ;
               // fall thru...
             case ERR_MESG_READ:
               if (b == '\r' || b == '\n')
                {
                  int nr = arg_buf.readableBytes ();

                  if (nr >= 2) // only do this if there are at least two bytes in the arg
                    {
                      // gnatsd puts single quotes around ERRs...
                      assert (arg_buf.readerIndex () == 0);
                      if (arg_buf.getByte (0) == '\'')
                        {
                          arg_buf.readerIndex (1);
                          nr--;
                        }
                      if (arg_buf.getByte (nr) == '\'')
                        arg_buf.writerIndex (nr);
                    }
                  ri--;
                  cur_state = HEADER_FINAL;
                  continue;
                }
               arg_buf.writeByte (b);
               continue;
             // CONN.ECT
             case CONNECT_VERB_E:
               cur_state = or (b, 'E', 'e', CONNECT_VERB_C);
               continue;
             case CONNECT_VERB_C:
               cur_state = or (b, 'C', 'c', CONNECT_VERB_T);
               continue;
             case CONNECT_VERB_T:
              cur_state = or (b, 'T', 't',  CONNECT_INFO_ARG);
              continue;
             case UNSU_VERB_B:
               cur_state = or (b, 'B', 'b', ARG_1);
               continue;

             // CONNECT|INFO.[\t ]*{
             case CONNECT_INFO_ARG:
               if (b == ' ' || b == '\t')
                {
                  cur_state = CONNECT_INFO_ARG_REST;
                  continue;
                }
               if (STRICT_PARSING_PER_SPEC) break;
               // fall thru, dlc code allows { after CONNECT/INFO without space...
             case CONNECT_INFO_ARG_REST:
               if (b == ' ' || b == '\t') continue;
               if (b != '{') break;
               json.clear ();
               cur_state = JSON_OBJ;
               continue;

            // INFO|CONNECT[ t]+{ "K":"V",* }
            case JSON_OBJ:
              if (b == '}') { ri--; cur_state = JSON_VE; continue; } // empty json {}
              if (b == ' ' || b == '\t') continue;
              if (b != '"') break;
              cur_state = JSON_K;
              arg_buf = argv[0];
              arg_buf.clear ();
              continue;

            case JSON_K:
            case JSON_V:
              if (b == '\n') break;
              if (b != '"' || arg_is_all_digits)
                {
                  arg_is_all_digits = (b == '\\');
                  arg_buf.writeByte (b);
                  continue;
                }
              final String v = arg_buf.toString (CharsetUtil.US_ASCII);
              if (cur_state == JSON_K)
                {
                  cur_state = JSON_SEP;
                  assert (json_arg == null);
                  json_arg = v;
                }
              else
                {
                  final String real_k = json_arg;

                  json_arg = null;
                  json.put (real_k, v);
                  cur_state = JSON_VE;
                }
              arg_buf.clear ();
              continue;
            // { K: . V }
            case JSON_SEPE:
              if (b == ' ' || b == '\t') continue;
              if (b == '"') { cur_state = JSON_V; arg_buf.clear (); continue; }
              cur_state = JSON_I;
              // fall thru
             case JSON_I:
               if ((b >= 'a' && b <= 'z')
                   || (b >= 'A' && b <= 'Z')
                   || (b >= '0' && b <= '9')
                   || b == '_')

                 {
                   arg_buf.writeByte (b);
                   continue;
                 }
               else
                 {
                   final String val = arg_buf.toString (CharsetUtil.US_ASCII);
                   final String real_k = json_arg;

                   json_arg = null;
                   cur_state = JSON_VE;
                   json.put (real_k, val);
                   arg_buf.clear ();
                   ri--;
                   continue;
                 }
            // { K . : V }
            case JSON_SEP:
              if (b == ' ' || b == '\t') continue;
              if (b == ':') { cur_state = JSON_SEPE; continue; }
              break;
            // { K : V . }
            case JSON_MAYBE_VAL:
              if (b == ' ' || b == '\t') continue;
              if (b == '"') { cur_state = JSON_K; continue; }
              if (b != '}') break;
              // fall thru...
            case JSON_VE:
              if (b == ' ' || b == '\t') continue;
              if (b == ',') { cur_state = JSON_MAYBE_VAL; continue; }
              if (b == '}')
                {
                  cur_state = HEADER_FINAL;
                  continue;
                }
               // fall thru...
             default:
               break;
            }
            fail (ctx, "protocol error: state=" + cur_state + " input=" + Integer.toString (b, 16)
                 + " ri=" + ri
                 + " wi=" + wi);
            return null;
          }
        assert (cur_state != FINAL_ERROR);
      }
    finally
      {
        buffer.readerIndex (ri);
        _saved_state = cur_state;
      }
    assert (cur_state == _saved_state);
    return null;
  }
}
