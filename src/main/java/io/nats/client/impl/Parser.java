package io.nats.client.impl;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import io.nats.client.ParseException;

public class Parser {
	private NATSConnection conn;
	private int state = 0;
	private final int DEFAULT_BUF_SIZE = 512;
	
	byte[] argBufBase = new byte[DEFAULT_BUF_SIZE];
	ByteBuffer argBufStream = null;
	
	byte[] msgBufBase = new byte[DEFAULT_BUF_SIZE];
	ByteBuffer msgBufStream = null;
	
	public class MsgArg {
		String 	subject;
		String 	reply;
		long	sid;
		int		size;
	}
	
	final static int MAX_CONTROL_LINE_SIZE = 1024;
	
	protected class ParseState {
		int	state;
		int	as;
		int	drop;
		MsgArg	ma;
		String	argBuf;
		String msgBuf;
		String scratch; 
	}
//	private static enum NatsOp {
//		OP_START,
//		OP_PLUS,
//		OP_PLUS_O,
//		OP_PLUS_OK,
//		OP_MINUS,
//		OP_MINUS_E,
//		OP_MINUS_ER,
//		OP_MINUS_ERR,
//		OP_MINUS_ERR_SPC,
//		MINUS_ERR_ARG,
//		OP_C,
//		OP_CO,
//		OP_CON,
//		OP_CONN,
//		OP_CONNE,
//		OP_CONNEC,
//		OP_CONNECT,
//		CONNECT_ARG,
//		OP_M,
//		OP_MS,
//		OP_MSG,
//		OP_MSG_SPC,
//		MSG_ARG,
//		MSG_PAYLOAD,
//		MSG_END,
//		OP_P,
//		OP_PI,
//		OP_PIN,
//		OP_PING,
//		OP_PO,
//		OP_PON,
//		OP_PONG
//	}
	private static class NatsOp {
        // For performance declare these as consts - they'll be
        // baked into the IL code (thus faster).  An enum would
        // be nice, but we want speed in this critical section of
        // message handling.
		private static Map<Integer, String> names = new HashMap<Integer, String>();
        final static int OP_START         = 0;
        static { names.put(OP_START, "OP_START");}  
        
        final static int OP_PLUS          = 1;
        static { names.put(OP_PLUS, "OP_PLUS");}        
        
        final static int OP_PLUS_O        = 2;
        static { names.put(OP_PLUS_O, "OP_PLUS_O");}        
        
	    final static int OP_PLUS_OK       = 3;
        static { names.put(OP_PLUS_OK, "OP_PLUS_OK");}   
        
	    final static int OP_MINUS         = 4;
        static { names.put(OP_MINUS, "OP_MINUS");}    
        
	    final static int OP_MINUS_E       = 5;
        static { names.put(OP_MINUS_E, "OP_MINUS_E");}   
        
	    final static int OP_MINUS_ER      = 6;
        static { names.put(OP_MINUS_ER, "OP_MINUS_ER");}   
        
	    final static int OP_MINUS_ERR     = 7;
        static { names.put(OP_MINUS_ERR, "OP_MINUS_ERR");} 
        
	    final static int OP_MINUS_ERR_SPC = 8;
        static { names.put(OP_MINUS_ERR_SPC, "OP_MINUS_ERR_SPC");} 
        
	    final static int MINUS_ERR_ARG    = 9;
        static { names.put(MINUS_ERR_ARG, "MINUS_ERR_ARG");}    
        
	    final static int OP_C             = 10;
        static { names.put(OP_C, "OP_C");}        
        
	    final static int OP_CO            = 11;
        static { names.put(OP_CO, "OP_CO");}   
        
	    final static int OP_CON           = 12;
        static { names.put(OP_CON, "OP_CON");}   
        
	    final static int OP_CONN          = 13;
        static { names.put(OP_CONN, "OP_CONN");}  
        
	    final static int OP_CONNE         = 14;
        static { names.put(OP_CONNE, "OP_CONNE");}   
        
	    final static int OP_CONNEC        = 15;
        static { names.put(OP_CONNEC, "OP_CONNEC");}  
        
	    final static int OP_CONNECT       = 16;
        static { names.put(OP_CONNECT, "OP_CONNECT");}    
        
	    final static int CONNECT_ARG      = 17; 
        static { names.put(CONNECT_ARG, "CONNECT_ARG");}    
        
	    final static int OP_M             = 18;
        static { names.put(OP_M, "OP_M");}        

	    final static int OP_MS            = 19;
        static { names.put(OP_MS, "OP_MS");}        
        
	    final static int OP_MSG           = 20;
        static { names.put(OP_MSG, "OP_MSG");}        

	    final static int OP_MSG_SPC       = 21;
        static { names.put(OP_MSG_SPC, "OP_MSG_SPC");}        

	    final static int MSG_ARG          = 22;
        static { names.put(MSG_ARG, "MSG_ARG");}        

	    final static int MSG_PAYLOAD      = 23;
        static { names.put(MSG_PAYLOAD, "MSG_PAYLOAD");}    
        
	    final static int MSG_END          = 24;
        static { names.put(MSG_END, "MSG_END");}
        
	    final static int OP_P             = 25;
        static { names.put(OP_P, "OP_P");}        
	    
	    final static int OP_PI            = 26;
        static { names.put(OP_PI, "OP_PI");}
        
	    final static int OP_PIN           = 27;
        static { names.put(OP_PIN, "OP_PIN");}
        
	    final static int OP_PING          = 28;
        static { names.put(OP_PING, "OP_PING");}
        
	    final static int OP_PO            = 29;
        static { names.put(OP_PO, "OP_PO");}
        
	    final static int OP_PON           = 30;
        static { names.put(OP_PON, "OP_PON");}
        
	    final static int OP_PONG          = 31;
        static { names.put(OP_PONG, "OP_PONG");}        
	}
	
	public Parser(NATSConnection nATSConnection) {
		argBufStream = ByteBuffer.wrap(argBufBase);
		msgBufStream = ByteBuffer.wrap(msgBufBase);
		this.conn = nATSConnection;
		this.conn.ps = null;
	}
	
	protected void parse(byte[] buffer, int len) throws ParseException {
		int i;
		char b;
		
        for (i = 0; i < len; i++)
        {
            b = (char)buffer[i];

            switch (state)
            {
                case NatsOp.OP_START:
                    switch (b)
                    {
                        case 'M':
                        case 'm':
                            state = NatsOp.OP_M;
                            break;
                        case 'C':
                        case 'c':
                            state = NatsOp.OP_C;
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
                            parseError(buffer,i);
                            break;
                    }
                    break;
                case NatsOp.OP_M:
                    switch (b)
                    {
                        case 'S':
                        case 's':
                            state = NatsOp.OP_MS;
                            break;
                        default:
                            parseError(buffer, i);
                            break;
                    }
                    break;
                case NatsOp.OP_MS:
                    switch (b)
                    {
                        case 'G':
                        case 'g':
                            state = NatsOp.OP_MSG;
                            break;
                        default:
                            parseError(buffer, i);
                            break;
                    }
                    break;
                case NatsOp.OP_MSG:
                    switch (b)
                    {
                        case ' ':
                        case '\t':
                            state = NatsOp.OP_MSG_SPC;
                            break;
                        default:
                            parseError(buffer, i);
                            break;
                    }
                    break;
                case NatsOp.OP_MSG_SPC:
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
                case NatsOp.MSG_ARG:
                    switch (b)
                    {
                        case '\r':
                            break;
                        case '\n':
                            conn.processMsgArgs(argBufBase, argBufStream.position());
                            argBufStream.position(0);
                            if (conn.msgArgs.size > msgBufBase.length)
                            {
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
                case NatsOp.MSG_PAYLOAD:
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
                case NatsOp.MSG_END:
                    switch (b)
                    {
                        case '\n':
                            state = NatsOp.OP_START;
                            break;
                        default:
                            continue;
                    }
                    break;
                case NatsOp.OP_PLUS:
                    switch (b)
                    {
                        case 'O':
                        case 'o':
                            state = NatsOp.OP_PLUS_O;
                            break;
                        default:
                            parseError(buffer, i);
                            break;
                    }
                    break;
                case NatsOp.OP_PLUS_O:
                    switch (b)
                    {
                        case 'K':
                        case 'k':
                            state = NatsOp.OP_PLUS_OK;
                            break;
                        default:
                            parseError(buffer, i);
                            break;
                    }
                    break;
                case NatsOp.OP_PLUS_OK:
                    switch (b)
                    {
                        case '\n':
                            conn.processOK();
                            state = NatsOp.OP_START;
                            break;
                    }
                    break;
                case NatsOp.OP_MINUS:
                    switch (b)
                    {
                        case 'E':
                        case 'e':
                            state = NatsOp.OP_MINUS_E;
                            break;
                        default:
                            parseError(buffer, i);
                            break;
                    }
                    break;
                case NatsOp.OP_MINUS_E:
                    switch (b)
                    {
                        case 'R':
                        case 'r':
                            state = NatsOp.OP_MINUS_ER;
                            break;
                        default:
                            parseError(buffer, i);
                            break;
                    }
                    break;
                case NatsOp.OP_MINUS_ER:
                    switch (b)
                    {
                        case 'R':
                        case 'r':
                            state = NatsOp.OP_MINUS_ERR;
                            break;
                        default:
                            parseError(buffer, i);
                            break;
                    }
                    break;
                case NatsOp.OP_MINUS_ERR:
                    switch (b)
                    {
                        case ' ':
                        case '\t':
                            state = NatsOp.OP_MINUS_ERR_SPC;
                            break;
                        default:
                            parseError(buffer, i);
                            break;
                    }
                    break;
                case NatsOp.OP_MINUS_ERR_SPC:
                    switch (b)
                    {
                        case ' ':
                        case '\t':
                            state = NatsOp.OP_MINUS_ERR_SPC;
                            break;
                        default:
                            state = NatsOp.MINUS_ERR_ARG;
                            break;
                    }
                    break;
                case NatsOp.MINUS_ERR_ARG:
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
                case NatsOp.OP_P:
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
                            parseError(buffer, i);
                            break;
                    }
                    break;
                case NatsOp.OP_PO:
                    switch (b)
                    {
                        case 'N':
                        case 'n':
                            state = NatsOp.OP_PON;
                            break;
                        default:
                            parseError(buffer, i);
                            break;
                    }
                    break;
                case NatsOp.OP_PON:
                    switch (b)
                    {
                        case 'G':
                        case 'g':
                            state = NatsOp.OP_PONG;
                            break;
                        default:
                            parseError(buffer, i);
                            break;
                    }
                    break;
                case NatsOp.OP_PONG:
                    switch (b)
                    {
                        case '\r':
                            break;
                        case '\n':
                            conn.processPong();
                            state = NatsOp.OP_START;
                            break;
                    }
                    break;
                case NatsOp.OP_PI:
                    switch (b)
                    {
                        case 'N':
                        case 'n':
                            state = NatsOp.OP_PIN;
                            break;
                        default:
                            parseError(buffer, i);
                            break;
                    }
                    break;
                case NatsOp.OP_PIN:
                    switch (b)
                    {
                        case 'G':
                        case 'g':
                            state = NatsOp.OP_PING;
                            break;
                        default:
                            parseError(buffer, i);
                            break;
                    }
                    break;
                case NatsOp.OP_PING:
                    switch (b)
                    {
                        case '\r':
                            break;
                        case '\n':
                            conn.processPing();
                            state = NatsOp.OP_START;
                            break;
                        default:
                            parseError(buffer, i);
                            break;
                    }
                    break;
                default:
                    throw new ParseException("Unable to parse.");
            } // switch(state)

        }  // for
		
	}
	
    private void parseError(byte[] buffer, int position) throws ParseException {
    	throw new ParseException(String.format("Parse Error [%s], %s", NatsOp.names.get(state), buffer));
    }


}
