import JobStatus from "../../../components/JobStatus";
import Layout from "../../../components/Layout";
import Link from "next/link";

export default function ResearchJobPage({ params }: { params: { id: string } }) {
  const { id } = params;

  return (
    <Layout
      layout="page"
      mainClassName="bg-horizon-light"
      showLogo={false}
      leftSlot={
        <Link
          href="/"
          className="inline-flex items-center gap-2 px-4 py-1.5 rounded-full border border-black/10 text-gray-600 hover:text-black hover:border-black/30 transition-all group font-mono uppercase tracking-wider text-[11px]"
        >
          <svg
            width="16"
            height="16"
            viewBox="0 0 16 16"
            fill="none"
            xmlns="http://www.w3.org/2000/svg"
            className="group-hover:-translate-x-1 transition-transform opacity-60 group-hover:opacity-100"
          >
            <path
              d="M3.828 7.00001H14V9.00001H3.828L9.192 14.364L7.778 15.778L0 8.00001L7.778 0.222015L9.192 1.63602L3.828 7.00001Z"
              fill="currentColor"
            />
          </svg>
          <span>Back</span>
        </Link>
      }
      rightSlot={
        <div className="flex items-center gap-6 text-[11px] font-mono uppercase tracking-widest">
          <Link
            href="/archive"
            className="text-gray-400 hover:text-black transition-colors"
          >
            View Archive
          </Link>
          <Link
            href="/"
            className="text-gray-400 hover:text-black transition-colors"
          >
            New Research
          </Link>
        </div>
      }
    >
      {/* Layout now gives us max-width and vertical spacing, so we don’t need extra centering here */}
      <JobStatus jobId={id} />
    </Layout>
  );
}
