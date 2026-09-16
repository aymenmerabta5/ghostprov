import type { WebsocketProvider } from "y-websocket";

interface AwarenessChange {
  readonly added: readonly number[];
  readonly updated: readonly number[];
  readonly removed: readonly number[];
}

/**
 * Y-Sweet already broadcasts accepted Awareness updates to every connection.
 * The pinned provider otherwise echoes remote states back over this socket,
 * placing bulk data ahead of the connection's own ACK/control publications.
 * Keep all receive/query behavior and broadcast only this provider's identity.
 */
export function withLocalAwarenessPublisher(
  provider: WebsocketProvider,
): WebsocketProvider {
  const original = provider._awarenessUpdateHandler;
  const publishLocal = (change: AwarenessChange, origin: unknown): void => {
    const id = provider.doc.clientID;
    const added = change.added.includes(id);
    const updated = change.updated.includes(id);
    const removed = change.removed.includes(id);
    if (!added && !updated && !removed) return;
    original.call(provider, {
      added: added ? [id] : [],
      updated: updated ? [id] : [],
      removed: removed ? [id] : [],
    }, origin);
  };

  provider.awareness.off("update", original);
  // WebsocketProvider.destroy() unregisters this slot. Replacing both the
  // listener and slot retains its existing lifecycle without another owner.
  provider._awarenessUpdateHandler = publishLocal;
  provider.awareness.on("update", publishLocal);
  return provider;
}
