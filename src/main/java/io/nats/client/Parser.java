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
	final static int MAX_CONTROL_LINE_SIZE = 1024;
	
	private ConnectionImpl nc;
	
	protected class ParseState {
		NatsOp state = NatsOp.OP_START;
//		int		state;
		int		as;
		int		drop;
		MsgArg	ma;

//		String 	argBuf;
		byte[] argBuf = new byte[DEFAULT_BUF_SIZE];
		ByteBuffer argBufStream = null;
		
//		String 	msgBuf;
		byte[] msgBuf = new byte[DEFAULT_BUF_SIZE];
		ByteBuffer msgBufStream = null;
		
		String 	scratch; 
	}
	
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
	
	protected ParseState ps = null;
	
	protected Parser(ConnectionImpl connectionImpl) {
		this.ps = new ParseState();
		ps.argBufStream = ByteBuffer.wrap(ps.argBuf);
		ps.msgBufStream = ByteBuffer.wrap(ps.msgBuf);
		this.nc = connectionImpl;
		this.nc.ps = ps;
	}
	
	protected void parse(byte[] buffer, int len) throws ParseException {
		int i;
		char b;
		boolean error = false;
		
        for (i = 0; i < len; i++)
        {
            b = (char)buffer[i];
//            System.err.println("STATE=" + ps.state + ", byte=" + b);
            switch (ps.state)
            {
                case OP_START:
                    switch (b)
                    {
                        case 'M':
                        case 'm':
                            ps.state = NatsOp.OP_M;
                            break;
                        case 'P':
                        case 'p':
                            ps.state = NatsOp.OP_P;
                            break;
                        case '+':
                            ps.state = NatsOp.OP_PLUS;
                            break;
                        case '-':
                            ps.state = NatsOp.OP_MINUS;
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
                            ps.state = NatsOp.OP_MS;
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
                            ps.state = NatsOp.OP_MSG;
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
                            ps.state = NatsOp.OP_MSG_SPC;
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
                            ps.state = NatsOp.MSG_ARG;
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
                            nc.processMsgArgs(ps.argBuf, ps.argBufStream.position());
                            ps.argBufStream.position(0);
                            if (nc.msgArgs.size > ps.msgBuf.length)
                            {
                            	// Add 2 to account for the \r\n
                                ps.msgBuf = new byte[nc.msgArgs.size+1];
                                ps.msgBufStream = ByteBuffer.wrap(ps.msgBuf);
                            }
                            ps.state = NatsOp.MSG_PAYLOAD;
                            break;
                        default:
                            ps.argBufStream.put((byte)b);
                            break;
                    }
                    break;
                case MSG_PAYLOAD:
                    long position = ps.msgBufStream.position();
                    if (position >= nc.msgArgs.size)
                    {
                        nc.processMsg(ps.msgBuf, position);
                        ps.msgBufStream.position(0);
                        ps.state = NatsOp.MSG_END;
                    }
                    else
                    {
                        ps.msgBufStream.put((byte)b);
                    }
                    break;
                case MSG_END:
                    switch (b)
                    {
                        case '\n':
                            ps.state = NatsOp.OP_START;
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
                            ps.state = NatsOp.OP_PLUS_O;
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
                            ps.state = NatsOp.OP_PLUS_OK;
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
                            nc.processOK();
                            ps.state = NatsOp.OP_START;
                            break;
                    }
                    break;
                case OP_MINUS:
                    switch (b)
                    {
                        case 'E':
                        case 'e':
                            ps.state = NatsOp.OP_MINUS_E;
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
                            ps.state = NatsOp.OP_MINUS_ER;
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
                            ps.state = NatsOp.OP_MINUS_ERR;
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
                            ps.state = NatsOp.OP_MINUS_ERR_SPC;
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
                            ps.state = NatsOp.OP_MINUS_ERR_SPC;
                            break;
                        default:
                            ps.state = NatsOp.MINUS_ERR_ARG;
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
                            nc.processErr(ps.argBufStream);
                            ps.argBufStream.position(0);
                            ps.state = NatsOp.OP_START;
                            break;
                        default:
                            ps.argBufStream.put((byte)b);
                            break;
                    }
                    break;
                case OP_P:
                    switch (b)
                    {
                        case 'I':
                        case 'i':
                            ps.state = NatsOp.OP_PI;
                            break;
                        case 'O':
                        case 'o':
                            ps.state = NatsOp.OP_PO;
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
                            ps.state = NatsOp.OP_PON;
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
                            ps.state = NatsOp.OP_PONG;
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
                            nc.processPong();
                            ps.state = NatsOp.OP_START;
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
                            ps.state = NatsOp.OP_PIN;
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
                            ps.state = NatsOp.OP_PING;
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
                            nc.processPing();
                            ps.state = NatsOp.OP_START;
                            break;
                        default:
                        	error=true;
                            break;
                    }
                    break;
               default:
                	break;
            } // switch(ps.state)
            if (error) {
            	error = false;
            	throw new ParseException(String.format("Parse Error [%s], [%s] [%d]", 
            			ps.state, new String(buffer),ps.argBufStream.position()),
            			ps.argBufStream.position());
            }
        }  // for
		
	}
}
