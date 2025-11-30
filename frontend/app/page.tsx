"use client";

import ResearchForm from "../components/ResearchForm";
import Layout from "../components/Layout";
import { useRouter } from "next/navigation";

export default function HomePage() {
  const router = useRouter();

  return (
    <Layout>
      <div className="w-full max-w-3xl">
        <ResearchForm onJobCreated={(id) => router.push(`/research/${id}`)} />
      </div>
    </Layout>
  );
}

