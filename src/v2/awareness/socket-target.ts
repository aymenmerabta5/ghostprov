export interface AwarenessSocketCredential {
  readonly url: string;
  readonly docId: string;
  readonly token: string;
}

/** Build one socket target from one credential snapshot without changing its document. */
export function awarenessSocketTarget(
  credential: AwarenessSocketCredential,
  expectedDocId: string,
): URL {
  if (credential.docId !== expectedDocId) {
    throw new TypeError("Awareness socket credential belongs to another document");
  }
  // Match the pinned WebsocketProvider's serverUrl + '/' + roomname assembly.
  // Read the current endpoint here; its stored serverUrl predates rotations.
  let end = credential.url.length;
  while (end > 0 && credential.url[end - 1] === "/") end--;
  const target = new URL(`${credential.url.slice(0, end)}/${credential.docId}`);
  target.searchParams.set("token", credential.token);
  return target;
}
