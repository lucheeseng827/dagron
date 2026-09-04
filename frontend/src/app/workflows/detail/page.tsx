"use client";

import { Suspense } from "react";
import { useRouteId } from "@/lib/route-id";
import WorkflowEditor from "@/components/WorkflowEditor";

export default function EditWorkflowPage() {
  // useRouteId() calls useSearchParams(), which suspends during prerender;
  // the static export build fails without this boundary.
  return (
    <Suspense fallback={null}>
      <EditWorkflowPageInner />
    </Suspense>
  );
}

function EditWorkflowPageInner() {
  const id = useRouteId();
  // No `?id=` means this URL names no workflow. WorkflowEditor reads a falsy id
  // as "new", so without this guard /workflows/detail/ silently becomes the
  // creation form — a different page than the one the link asked for. Creation
  // has its own route (/workflows/new).
  if (!id) return <p className="dy-empty">Missing workflow ID.</p>;
  return <WorkflowEditor id={id} />;
}
