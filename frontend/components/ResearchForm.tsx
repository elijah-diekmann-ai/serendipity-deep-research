"use client";

import { useState } from "react";
import axios from "axios";
import { API_BASE_URL } from "../lib/api";

const MAX_CONTEXT_LENGTH = 4000;

function buildPayloadFromPrompt(prompt: string) {
  // 1) Try to extract a URL to use as website
  // Supports http(s)://... and common domain patterns (example.com, www.example.com)
  const urlMatch = prompt.match(
    /(https?:\/\/[^\s)]+)|((?:www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b(?:[-a-zA-Z0-9()@:%_\+.~#?&//=]*))/
  );
  const website = urlMatch ? urlMatch[0] : null;

  // 2) Naive company name extraction
  let company_name = "";
  const quotedMatch = prompt.match(/"([^"]+)"/);
  if (quotedMatch) {
    company_name = quotedMatch[1];
  } else {
    const firstLine = prompt.split("\n")[0];
    const stopChars = [".", "-", "—", ":", "("];
    let endIndex = firstLine.length;

    for (const ch of stopChars) {
      const idx = firstLine.indexOf(ch);
      if (idx !== -1 && idx < endIndex) {
        endIndex = idx;
      }
    }

    company_name = firstLine
      .slice(0, endIndex)
      .replace(/^[\sA-Za-z ]*on\s+/i, "")
      .trim();

    if (!company_name) {
      company_name = firstLine.trim();
    }

    // Clean up common trailing punctuation from naive extraction
    company_name = company_name.replace(/[.,;)]+$/, "");
  }

  // 3) Use the entire prompt as context
  const context = prompt.trim();

  // Clean up website (remove trailing parentheses or punctuation if regex grabbed too much)
  let cleanWebsite = website;
  if (cleanWebsite) {
    cleanWebsite = cleanWebsite.replace(/[.,;)]+$/, "");
  }

  return { company_name, website: cleanWebsite, context };
}

type Props = {
  onJobCreated: (jobId: string) => void;
};

export default function ResearchForm({ onJobCreated }: Props) {
  const [prompt, setPrompt] = useState("");
  const [overrideCompanyName, setOverrideCompanyName] = useState("");
  const [overrideWebsite, setOverrideWebsite] = useState("");
  const [loading, setLoading] = useState(false);
  const [isExiting, setIsExiting] = useState(false);
  const [promptError, setPromptError] = useState<string | null>(null);
  const [showAdvanced, setShowAdvanced] = useState(false);

  const inferred = buildPayloadFromPrompt(prompt);
  const effectiveCompanyName =
    overrideCompanyName.trim() || inferred.company_name || "";
  const effectiveWebsite = overrideWebsite.trim() || inferred.website || "";

  async function handleSubmit(e?: React.FormEvent) {
    if (e) e.preventDefault();
    const trimmed = prompt.trim();
    if (!trimmed) return;

    if (trimmed.length > MAX_CONTEXT_LENGTH) {
      setPromptError(
        `Prompt is too long (${trimmed.length}/${MAX_CONTEXT_LENGTH} characters). Please shorten it.`
      );
      return;
    }

    setPromptError(null);
    setLoading(true);

    try {
      const autoPayload = buildPayloadFromPrompt(trimmed);

      const company_name =
        overrideCompanyName.trim() || autoPayload.company_name;
      const websiteValue = overrideWebsite.trim() || autoPayload.website || null;

      const resp = await axios.post(`${API_BASE_URL}/research`, {
        company_name,
        website: websiteValue,
        context: trimmed,
      });

      // Start exit animation
      setIsExiting(true);
      
      // Delay navigation to allow animation to play
      setTimeout(() => {
        onJobCreated(resp.data.id);
      }, 800);
      
    } catch (error) {
      console.error("Failed to create job", error);
      alert("Failed to create job");
      setLoading(false);
    }
  }

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSubmit();
    }
  };

  return (
    <div
      className={`horizon-container relative w-full max-w-3xl mx-auto px-4 transition-all duration-700 ease-in-out ${
        isExiting ? "opacity-0 scale-95 blur-sm" : "opacity-100 scale-100 blur-0"
      }`}
    >
      {/* Chat Input Interface */}
      <form onSubmit={handleSubmit} className="chat-interface w-full relative">
        <div className="input-wrapper relative">
          <div className="glass-bar">
            <textarea
              className="chat-input font-sans resize-none overflow-hidden h-auto min-h-[1.5em] max-h-[200px]"
              placeholder="Enter query, person, or company name..."
              value={prompt}
              onChange={(e) => {
                setPrompt(e.target.value);
                e.target.style.height = 'auto';
                e.target.style.height = `${e.target.scrollHeight}px`;
              }}
              onKeyDown={(e) => {
                if (e.key === "Enter" && !e.shiftKey) {
                  e.preventDefault();
                  handleSubmit();
                }
              }}
              disabled={loading}
              autoFocus
              rows={1}
            />
            <div className="text-[10px] font-mono text-white/30 whitespace-nowrap ml-4 tracking-wider self-end mb-2">
              [Press Enter to Run]
            </div>
          </div>
        </div>

        {/* Line Data Metadata */}
        <div className="mt-4 font-mono text-xs text-white/40 flex flex-wrap items-center gap-2 cursor-pointer hover:text-white/60 transition-colors" onClick={() => setShowAdvanced(!showAdvanced)}>
          <span className="hover:underline decoration-white/30 underline-offset-4">
            Target: {effectiveCompanyName || "—"}
          </span>
          <span className="text-white/20">/</span>
          <span className="hover:underline decoration-white/30 underline-offset-4">
            Website: {effectiveWebsite || "—"}
          </span>
          <span className="text-white/20">/</span>
          <span className="text-[10px]">
            {prompt.length}/{MAX_CONTEXT_LENGTH}
          </span>
        </div>

        {/* Advanced Fields (Hidden by default) */}
        {showAdvanced && (
          <div className="mt-4 grid grid-cols-2 gap-4 pt-4 border-t border-white/10 font-mono text-xs">
            <div>
              <label className="block mb-2 text-white/40 uppercase tracking-wider text-[10px]">
                Company Name Override
              </label>
              <input
                type="text"
                className="w-full bg-transparent border-b border-white/20 py-1 text-white/80 focus:border-white/60 focus:outline-none transition-colors placeholder:text-white/10"
                placeholder="Inferred name..."
                value={overrideCompanyName}
                onChange={(e) => setOverrideCompanyName(e.target.value)}
              />
            </div>
            <div>
              <label className="block mb-2 text-white/40 uppercase tracking-wider text-[10px]">
                Website Override
              </label>
              <input
                type="text"
                className="w-full bg-transparent border-b border-white/20 py-1 text-white/80 focus:border-white/60 focus:outline-none transition-colors placeholder:text-white/10"
                placeholder="https://example.com"
                value={overrideWebsite}
                onChange={(e) => setOverrideWebsite(e.target.value)}
              />
            </div>
          </div>
        )}

        {promptError && (
          <div className="mt-4 text-xs font-mono text-red-400 bg-red-900/10 py-2 px-3 border-l-2 border-red-500/50">
            {promptError}
          </div>
        )}
      </form>
    </div>
  );
}
