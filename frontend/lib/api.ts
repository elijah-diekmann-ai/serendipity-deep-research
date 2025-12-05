import axios from 'axios';

export const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:8000/api";

export const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// ---------------------------------------------------------------------------
// Q&A Types
// ---------------------------------------------------------------------------

export type JobQAPair = {
  id: number;
  job_id: string;
  question: string;
  answer_markdown: string;
  used_source_ids?: number[] | null;
  created_at: string;
};

// ---------------------------------------------------------------------------
// Micro-Research Plan Types
// ---------------------------------------------------------------------------

export type ResearchPlanProposal = {
  plan_id: string;
  gap_statement: string;
  plan_markdown: string;
  estimated_cost: { label: string };
  estimated_runtime: { label: string };
  action: string;
};

export type JobQAPairExtended = JobQAPair & {
  research_plan?: ResearchPlanProposal | null;
};

// ---------------------------------------------------------------------------
// Source Types (for citation syncing)
// ---------------------------------------------------------------------------

export type SourceOut = {
  id: number;
  url?: string | null;
  title?: string | null;
  provider: string;
  published_date?: string | null;
};

// ---------------------------------------------------------------------------
// Q&A API Functions
// ---------------------------------------------------------------------------

export async function fetchJobQA(jobId: string): Promise<JobQAPair[]> {
  const resp = await api.get<JobQAPair[]>(`/research/${jobId}/qa`);
  return resp.data;
}

export async function askJobQuestion(
  jobId: string,
  question: string
): Promise<JobQAPairExtended> {
  const resp = await api.post<JobQAPairExtended>(`/research/${jobId}/qa`, { question });
  return resp.data;
}

// ---------------------------------------------------------------------------
// Micro-Research API Functions
// ---------------------------------------------------------------------------

/**
 * Execute a proposed micro-research plan.
 * 
 * @param jobId - The research job ID
 * @param planId - The micro-research plan ID to execute
 * @returns The new Q&A response with updated answer
 */
export async function runMicroResearch(
  jobId: string,
  planId: string
): Promise<JobQAPairExtended> {
  const resp = await api.post<JobQAPairExtended>(
    `/research/${jobId}/qa/research/${planId}/run`
  );
  return resp.data;
}

/**
 * Fetch all sources for a job.
 * Used to sync citations after micro-research adds new sources.
 * 
 * @param jobId - The research job ID
 * @returns List of all sources for the job
 */
export async function fetchJobSources(jobId: string): Promise<SourceOut[]> {
  const resp = await api.get<SourceOut[]>(`/research/${jobId}/sources`);
  return resp.data;
}

export type ArchiveJob = {
  job: {
    id: string;
    status: string;
    created_at: string;
    completed_at: string | null;
    target_input: {
      target_type?: string;
      company_name?: string;
      person_name?: string;
      context?: string;
    };
  };
  has_brief: boolean;
};

export async function fetchArchive(limit = 50, offset = 0): Promise<ArchiveJob[]> {
  const resp = await api.get<ArchiveJob[]>(`/archive`, {
    params: { limit, offset },
  });
  return resp.data;
}
