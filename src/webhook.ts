export const createAgentInboxWebhookHandler =
  (
    ingest: (input: {
      source: string;
      eventId: string;
      kind: string;
      body: Uint8Array;
      headers: Headers;
    }) => Promise<unknown>,
    options: {
      source: string;
      eventIdHeader?: string;
      kindHeader?: string;
      maxBodyBytes?: number;
    },
  ) =>
  async (request: Request): Promise<Response> => {
    if (request.method !== "POST")
      return new Response("method not allowed", { status: 405 });
    const length = Number(request.headers.get("content-length") ?? 0);
    if (length > (options.maxBodyBytes ?? 1_048_576))
      return new Response("payload too large", { status: 413 });
    const eventId = request.headers.get(options.eventIdHeader ?? "x-event-id");
    const kind = request.headers.get(options.kindHeader ?? "x-event-type");
    if (!eventId || !kind)
      return new Response("missing event identity", { status: 400 });
    try {
      const body = new Uint8Array(await request.arrayBuffer());
      if (body.byteLength > (options.maxBodyBytes ?? 1_048_576))
        return new Response("payload too large", { status: 413 });
      await ingest({
        source: options.source,
        eventId,
        kind,
        body,
        headers: request.headers,
      });
      return new Response(null, { status: 202 });
    } catch (error) {
      return error instanceof AgentInboxVerificationError
        ? new Response("verification failed", { status: 401 })
        : new Response("inbox unavailable", { status: 503 });
    }
  };
import { AgentInboxVerificationError } from "./service";
