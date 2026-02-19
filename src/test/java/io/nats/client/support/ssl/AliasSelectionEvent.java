package io.nats.client.support.ssl;

import java.util.Date;

/**
 * Records a key manager alias selection — which alias was chosen
 * (or {@code null} if none was found) for client or server authentication.
 */
public class AliasSelectionEvent {
    public final Date timestamp;
    /** "chooseClientAlias" or "chooseServerAlias" */
    public final String operation;
    public final String[] keyTypes;
    /** The chosen alias, or {@code null} if none was found */
    public final String chosenAlias;

    AliasSelectionEvent(String operation, String[] keyTypes, String chosenAlias) {
        this.timestamp = new Date();
        this.operation = operation;
        this.keyTypes = keyTypes != null ? keyTypes.clone() : null;
        this.chosenAlias = chosenAlias;
    }

    AliasSelectionEvent(String operation, String keyType, String chosenAlias) {
        this(operation, new String[]{keyType}, chosenAlias);
    }
}
