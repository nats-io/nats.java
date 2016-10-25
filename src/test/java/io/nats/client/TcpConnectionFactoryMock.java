package io.nats.client;

public class TcpConnectionFactoryMock extends TcpConnectionFactory {

    private Options o;
    private String infoString;
    private boolean openFailure;
    private boolean noPongs;
    private boolean badReader;
    private boolean badWriter;
    private boolean verboseNoOK;
    private boolean sendNullPong;
    private boolean sendGenericError;
    private boolean sendAuthorizationError;
    private boolean closeStream;
    private boolean noInfo;
    private boolean tlsRequired;

    public TcpConnectionFactoryMock() {
        o = new Options();
    }

    protected TcpConnectionMock createConnection() {
        TcpConnectionMock tcpConnMock = new TcpConnectionMock();
        if (infoString != null) {
            tcpConnMock.setServerInfoString(infoString);
        }
        tcpConnMock.setOpenFailure(openFailure);
        tcpConnMock.setNoPongs(noPongs);
        tcpConnMock.setBadReader(badReader);
        tcpConnMock.setBadWriter(badWriter);
        tcpConnMock.setVerboseNoOK(verboseNoOK);
        tcpConnMock.setSendNullPong(sendNullPong);
        tcpConnMock.setSendGenericError(sendGenericError);
        tcpConnMock.setSendAuthorizationError(sendAuthorizationError);
        tcpConnMock.setCloseStream(closeStream);
        tcpConnMock.setNoInfo(noInfo);
        tcpConnMock.setTlsRequired(tlsRequired);
        return tcpConnMock;
    }

    public void setServerInfoString(String infoString) {
        this.infoString = infoString;
    }

    public void setOpenFailure(boolean b) {
        this.openFailure = b;
    }

    public void setNoPongs(boolean b) {
        this.noPongs = b;
    }

    public void setBadReader(boolean b) {
        this.badReader = b;
    }

    public void setBadWriter(boolean b) {
        this.badWriter = b;
    }

    public void setVerboseNoOK(boolean b) {
        this.verboseNoOK = b;
    }

    public void setSendNullPong(boolean b) {
        this.sendNullPong = b;
    }

    public void setSendGenericError(boolean b) {
        this.sendGenericError = b;
    }

    public void setSendAuthorizationError(boolean b) {
        this.sendAuthorizationError = b;
    }

    public void setCloseStream(boolean b) {
        this.closeStream = b;
    }

    public void setNoInfo(boolean b) {
        this.noInfo = b;
    }

    public void setTlsRequired(boolean b) {
        this.tlsRequired = b;
    }
}
