package io.nats.client;

public class TCPConnectionFactoryMock extends TCPConnectionFactory {

    private Options o;
    private String infoString;
    private boolean openFailure;
    private boolean noPongs;
    private boolean throwTimeoutException;
    private boolean badReader;
    private boolean badWriter;
    private boolean verboseNoOK;
    private boolean sendNullPong;
    private boolean sendGenericError;
    private boolean sendAuthorizationError;
    private boolean closeStream;
    private boolean noInfo;
    private boolean tlsRequired;

    public TCPConnectionFactoryMock() {
        o = new Options();
    }

    protected TCPConnectionMock createConnection() {
        TCPConnectionMock m = new TCPConnectionMock();
        if (infoString != null)
            m.setServerInfoString(infoString);
        m.setOpenFailure(openFailure);
        m.setNoPongs(noPongs);
        m.setThrowTimeoutException(throwTimeoutException);
        m.setBadReader(badReader);
        m.setBadWriter(badWriter);
        m.setVerboseNoOK(verboseNoOK);
        m.setSendNullPong(sendNullPong);
        m.setSendGenericError(sendGenericError);
        m.setSendAuthorizationError(sendAuthorizationError);
        m.setCloseStream(closeStream);
        m.setNoInfo(noInfo);
        m.setTlsRequired(tlsRequired);
        return m;
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

    public void setThrowTimeoutException(boolean b) {
        this.throwTimeoutException = b;

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
