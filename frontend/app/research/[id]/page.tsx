"use client";

import JobStatus from "../../../components/JobStatus";
import Layout from "../../../components/Layout";

export default function ResearchJobPage({ params }: { params: { id: string } }) {
  const { id } = params;

  return (
    <Layout
      layout="page"
      mainClassName="bg-horizon-light"
      showSidebar={true}
      hasRightPanel={true}
      currentJobId={id}
    >
      <JobStatus jobId={id} />
    </Layout>
  );
}
