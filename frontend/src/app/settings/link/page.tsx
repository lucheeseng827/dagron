"use client";

// Thin route over the fleet-link screen. `@ee/FleetLinkView` resolves to the
// enterprise implementation (src/ee) when present, else to the signpost in
// src/ee-stub, so this file compiles either way.

import FleetLinkView from "@ee/FleetLinkView";

export default function FleetLinkPage() {
  return <FleetLinkView />;
}
