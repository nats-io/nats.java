/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import static io.nats.client.ConnectionImpl.DEFAULT_BUF_SIZE;

final class Parser {
	/**
	 * Maximum size of a control line (message header)
	 */
	final static int 		MAX_CONTROL_LINE_SIZE = 1024;
	
	private ConnectionImpl conn;
//	protected int state = 0;
	NatsOp state = NatsOp.OP_START;
	
	byte[] argBufBase = new byte[DEFAULT_BUF_SIZE];
	ByteBuffer argBufStream = null;
	
	byte[] msgBufBase = new byte[DEFAULT_BUF_SIZE];
	ByteBuffer msgBufStream = null;
	
//	protected class ParseState {
//		int		state;
//		int		as;
//		int		drop;
//		MsgArg	ma;
//		String 	argBuf;
//		String 	msgBuf;
//		String 	scratch; 
//	}

	static enum NatsOp {
	    		OP_START,
	    	    OP_PLUS,
	    	    OP_PLUS_O,
	    	    OP_PLUS_OK,
	    	    OP_MINUS,
	    	    OP_MINUS_E,
	    	    OP_MINUS_ER,
	    	    OP_MINUS_ERR,
	    	    OP_MINUS_ERR_SPC,
	    	    MINUS_ERR_ARG,
	    	    OP_M,
	    	    OP_MS,
	    	    OP_MSG,
	    	    OP_MSG_SPC,
	    	    MSG_ARG,
	    	    MSG_PAYLOAD,
	    	    MSG_END,
	    	    OP_P,
	    	    OP_PI,
	    	    OP_PIN,
	    	    OP_PING,
	    	    OP_PO,
	    	    OP_PON,
	    	    OP_PONG
	}
	
	public Parser(ConnectionImpl connectionImpl) {
		argBufStream = ByteBuffer.wrap(argBufBase);
		msgBufStream = ByteBuffer.wrap(msgBufBase);
		this.conn = connectionImpl;
		this.conn.ps = null;
	}
	
	protected void parse(byte[] buffer, int len) throws ParseException {
		int i;
		char b;
		boolean error = false;
		
        for (i = 0; i < len; i++)
        {
            b = (char)buffer[i];
//            System.err.println("STATE=" + state + ", byte=" + b);
            switch (state)
            {
                case OP_START:
                    switch (b)
                    {
                        case 'M':
                        case 'm':
                            state = NatsOp.OP_M;
                            break;
                        case 'P':
                        case 'p':
                            state = NatsOp.OP_P;
                            break;
                        case '+':
                            state = NatsOp.OP_PLUS;
                            break;
                        case '-':
                            state = NatsOp.OP_MINUS;
                            break;
                        default:
                            error=true;
                            break;
                    }
                    break;
                case OP_M:
                    switch (b)
                    {
                        case 'S':
                        case 's':
                            state = NatsOp.OP_MS;
                            break;
                        default:
                            error=true;
                            break;
                    }
                    break;
                case OP_MS:
                    switch (b)
                    {
                        case 'G':
                        case 'g':
                            state = NatsOp.OP_MSG;
                            break;
                        default:
                            error=true;
                            break;
                    }
                    break;
                case OP_MSG:
                    switch (b)
                    {
                        case ' ':
                        case '\t':
                            state = NatsOp.OP_MSG_SPC;
                            break;
                        default:
                            error=true;
                            break;
                    }
                    break;
                case OP_MSG_SPC:
                    switch (b)
                    {
                        case ' ':
                            break;
                        case '\t':
                            break;
                        default:
                            state = NatsOp.MSG_ARG;
                            i--;
                            break;
                    }
                    break;
                case MSG_ARG:
                    switch (b)
                    {
                        case '\r':
                            break;
                        case '\n':
                            conn.processMsgArgs(argBufBase, argBufStream.position());
                            argBufStream.position(0);
                            if (conn.msgArgs.size > msgBufBase.length)
                            {
                            	// Add 2 to account for the \r\n
                                msgBufBase = new byte[conn.msgArgs.size+1];
                                msgBufStream = ByteBuffer.wrap(msgBufBase);
                            }
                            state = NatsOp.MSG_PAYLOAD;
                            break;
                        default:
                            argBufStream.put((byte)b);
                            break;
                    }
                    break;
                case MSG_PAYLOAD:
                    long position = msgBufStream.position();
                    if (position >= conn.msgArgs.size)
                    {
                        conn.processMsg(msgBufBase, position);
                        msgBufStream.position(0);
                        state = NatsOp.MSG_END;
                    }
                    else
                    {
                        msgBufStream.put((byte)b);
                    }
                    break;
                case MSG_END:
                    switch (b)
                    {
                        case '\n':
                            state = NatsOp.OP_START;
                            break;
                        default:
                            continue;
                    }
                    break;
                case OP_PLUS:
                    switch (b)
                    {
                        case 'O':
                        case 'o':
                            state = NatsOp.OP_PLUS_O;
                            break;
                        default:
                            error=true;
                            break;
                    }
                    break;
                case OP_PLUS_O:
                    switch (b)
                    {
                        case 'K':
                        case 'k':
                            state = NatsOp.OP_PLUS_OK;
                            break;
                        default:
                            error=true;
                            break;
                    }
                    break;
                case OP_PLUS_OK:
                    switch (b)
                    {
                        case '\n':
                            conn.processOK();
                            state = NatsOp.OP_START;
                            break;
                    }
                    break;
                case OP_MINUS:
                    switch (b)
                    {
                        case 'E':
                        case 'e':
                            state = NatsOp.OP_MINUS_E;
                            break;
                        default:
                            error=true;
                            break;
                    }
                    break;
                case OP_MINUS_E:
                    switch (b)
                    {
                        case 'R':
                        case 'r':
                            state = NatsOp.OP_MINUS_ER;
                            break;
                        default:
                            error=true;
                            break;
                    }
                    break;
                case OP_MINUS_ER:
                    switch (b)
                    {
                        case 'R':
                        case 'r':
                            state = NatsOp.OP_MINUS_ERR;
                            break;
                        default:
                            error=true;
                            break;
                    }
                    break;
                case OP_MINUS_ERR:
                    switch (b)
                    {
                        case ' ':
                        case '\t':
                            state = NatsOp.OP_MINUS_ERR_SPC;
                            break;
                        default:
                            error=true;
                            break;
                    }
                    break;
                case OP_MINUS_ERR_SPC:
                    switch (b)
                    {
                        case ' ':
                        case '\t':
                            state = NatsOp.OP_MINUS_ERR_SPC;
                            break;
                        default:
                            state = NatsOp.MINUS_ERR_ARG;
                            i--;
                            break;
                    }
                    break;
                case MINUS_ERR_ARG:
                    switch (b)
                    {
                        case '\r':
                            break;
                        case '\n':
                            conn.processErr(argBufStream);
                            argBufStream.position(0);
                            state = NatsOp.OP_START;
                            break;
                        default:
                            argBufStream.put((byte)b);
                            break;
                    }
                    break;
                case OP_P:
                    switch (b)
                    {
                        case 'I':
                        case 'i':
                            state = NatsOp.OP_PI;
                            break;
                        case 'O':
                        case 'o':
                            state = NatsOp.OP_PO;
                            break;
                        default:
                            error=true;
                            break;
                    }
                    break;
                case OP_PO:
                    switch (b)
                    {
                        case 'N':
                        case 'n':
                            state = NatsOp.OP_PON;
                            break;
                        default:
//                            parseError(buffer, i);
                        	error=true;
                            break;
                    }
                    break;
                case OP_PON:
                    switch (b)
                    {
                        case 'G':
                        case 'g':
                            state = NatsOp.OP_PONG;
                            break;
                        default:
                        	error=true;
                            break;
                    }
                    break;
                case OP_PONG:
                    switch (b)
                    {
                        case '\r':
                            break;
                        case '\n':
                            conn.processPong();
                            state = NatsOp.OP_START;
                            break;
                        default:
                        	error=true;
                        	break;
                    }
                    break;
                case OP_PI:
                    switch (b)
                    {
                        case 'N':
                        case 'n':
                            state = NatsOp.OP_PIN;
                            break;
                        default:
                        	error=true;
                            break;
                    }
                    break;
                case OP_PIN:
                    switch (b)
                    {
                        case 'G':
                        case 'g':
                            state = NatsOp.OP_PING;
                            break;
                        default:
                        	error=true;
                            break;
                    }
                    break;
                case OP_PING:
                    switch (b)
                    {
                        case '\r':
                            break;
                        case '\n':
                            conn.processPing();
                            state = NatsOp.OP_START;
                            break;
                        default:
                        	error=true;
                            break;
                    }
                    break;
               default:
                	break;
            } // switch(state)
            if (error) {
            	error = false;
            	throw new ParseException(String.format("Parse Error [%s], [%s] [%d]", 
            			state, new String(buffer),argBufStream.position()),
            			argBufStream.position());
            }
        }  // for
		
	}
}
