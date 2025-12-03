import axios from 'axios';

export const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:8000/api";

export const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

export type JobQAPair = {
  id: number;
  job_id: string;
  question: string;
  answer_markdown: string;
  used_source_ids?: number[] | null;
  created_at: string;
};

export async function fetchJobQA(jobId: string): Promise<JobQAPair[]> {
  const resp = await api.get<JobQAPair[]>(`/research/${jobId}/qa`);
  return resp.data;
}

export async function askJobQuestion(
  jobId: string,
  question: string
): Promise<JobQAPair> {
  const resp = await api.post<JobQAPair>(`/research/${jobId}/qa`, { question });
  return resp.data;
}
