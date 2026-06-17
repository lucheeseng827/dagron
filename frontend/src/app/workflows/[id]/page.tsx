"use client";

import { use } from "react";
import WorkflowEditor from "@/components/WorkflowEditor";

export default function EditWorkflowPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = use(params);
  return <WorkflowEditor id={id} />;
}
