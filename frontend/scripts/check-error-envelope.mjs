#!/usr/bin/env node
// Hold the console's error parsing to the API's two error shapes.
//
//     npm run check:errors
//
// `docs/API.md` says handlers answer `{"error": "<message>"}`. Some do — the workflow
// refusals, the viewer read-only gate, `archive.rs`, the 404 fallback — and most still
// answer a plain-text body. Issue #1196 tracks migrating the rest, module by module, so
// this parser has to keep both shapes right for as long as that takes.
//
// It matters because the message reaches a user: read the raw body and a JSON handler
// shows `403: {"error":"viewer role is read-only"}` in the console. Read only the
// envelope and a plain-text handler shows nothing at all.
//
// The fallbacks are the interesting half. A body that is JSON but not an envelope, or
// whose `error` is not a non-empty string, has to come back as the raw text rather than
// as "undefined" or an empty message — the user is better served by an ugly body than by
// a blank one.
//
// No test framework, matching `check-loops.mjs` and `check-recipe-vectors.mjs`.

import assert from "node:assert/strict";
import { register } from "node:module";

register("./alias-hook.mjs", import.meta.url);

const { errorBody } = await import("@/lib/err");

/// The bits of `Response` the parser touches.
const res = (body, statusText = "Forbidden") => ({
  text: async () => body,
  statusText,
});

let checks = 0;
const shows = async (name, body, want) => {
  assert.equal(await errorBody(res(body)), want, name);
  checks += 1;
};

// The envelope, which is what the migration produces.
await shows("envelope", '{"error":"viewer role is read-only"}', "viewer role is read-only");
await shows(
  "envelope with the extra fields a 409 carries",
  '{"error":"managed by a repo","repo":"git@host:o/r.git","sync_to_git":"/api/x"}',
  "managed by a repo",
);

// Plain text, which is still most of the API.
await shows("plain text", "admin group required", "admin group required");

// Fallbacks: anything that is not a usable envelope shows the body it came with.
await shows("JSON without an `error` key", '{"detail":"nope"}', '{"detail":"nope"}');
await shows("`error` that is not a string", '{"error":{"code":1}}', '{"error":{"code":1}}');
await shows("`error` that is empty", '{"error":""}', '{"error":""}');
await shows("JSON that does not parse", '{"error": broken', '{"error": broken');
await shows("a bare JSON string", '"just a string"', '"just a string"');

// An empty body is the one case with nothing to show, so the status line stands in.
assert.equal(await errorBody(res("", "Bad Gateway")), "Bad Gateway", "empty body falls back to statusText");
checks += 1;

// A body the fetch never delivered must not throw on the way to the message.
assert.equal(
  await errorBody({ text: async () => { throw new Error("stream closed"); }, statusText: "Forbidden" }),
  "Forbidden",
  "a body that cannot be read still yields a message",
);
checks += 1;

console.log(`error envelope: ${checks} checks passed`);
