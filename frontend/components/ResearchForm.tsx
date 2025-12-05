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

  // 2) Naive company name extraction - remove website first to avoid pollution
  let promptWithoutUrl = prompt;
  if (website) {
    promptWithoutUrl = prompt.replace(website, "").trim();
  }

  let company_name = "";
  const quotedMatch = promptWithoutUrl.match(/"([^"]+)"/);
  if (quotedMatch) {
    company_name = quotedMatch[1];
  } else {
    const firstLine = promptWithoutUrl.split("\n")[0];
    const stopChars = ["-", "—", ":", "("];
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
  const [targetType, setTargetType] = useState<"company" | "person">("company");
  const [personName, setPersonName] = useState("");
  const [personAffiliation, setPersonAffiliation] = useState("");
  const [personLocation, setPersonLocation] = useState("");

  const inferred = buildPayloadFromPrompt(prompt);
  const effectiveCompanyName =
    overrideCompanyName.trim() || inferred.company_name || "";
  const effectiveWebsite = overrideWebsite.trim() || inferred.website || "";
  const displayPersonName =
    personName.trim() ||
    overrideCompanyName.trim() ||
    inferred.company_name ||
    "";

  const handleModeChange = (mode: "company" | "person") => {
    setTargetType(mode);
    setPromptError(null);
    if (mode === "person" && !personName) {
      const seed = overrideCompanyName.trim() || inferred.company_name || "";
      if (seed) {
        setPersonName(seed);
      }
    }
  };

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

      const inferredCompany =
        overrideCompanyName.trim() || autoPayload.company_name || "";
      const websiteValue =
        overrideWebsite.trim() || autoPayload.website || null;

      const requestBody: Record<string, any> = {
        target_type: targetType,
        context: trimmed,
      };

      if (websiteValue) {
        requestBody.website = websiteValue;
      }

      if (targetType === "company") {
        if (!inferredCompany) {
          setPromptError("Please provide a company name or override.");
          setLoading(false);
          return;
        }
        requestBody.company_name = inferredCompany;
      } else {
        const resolvedPersonName =
          personName.trim() || inferredCompany || "";
        if (!resolvedPersonName) {
          setPromptError("Please provide a person name to research.");
          setLoading(false);
          return;
        }
        requestBody.person_name = resolvedPersonName;
        if (personAffiliation.trim()) {
          requestBody.company_name = personAffiliation.trim();
        } else if (inferredCompany) {
          requestBody.company_name = inferredCompany;
        }
        if (personLocation.trim()) {
          requestBody.location = personLocation.trim();
        }
      }

      const resp = await axios.post(`${API_BASE_URL}/research`, requestBody);

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
      <form onSubmit={handleSubmit} className="chat-interface w-full relative pb-4">
        <div className="flex items-center gap-2 text-[11px] uppercase tracking-wider font-mono text-white/80 mb-3">
          <span>Researching:</span>
          <button
            type="button"
            onClick={() => handleModeChange("company")}
            className={`px-3 py-1 rounded-full border transition-colors ${
              targetType === "company"
                ? "border-white/90 text-white"
                : "border-white/30 text-white/50 hover:text-white/80"
            }`}
          >
            Company
          </button>
          <button
            type="button"
            onClick={() => handleModeChange("person")}
            className={`px-3 py-1 rounded-full border transition-colors ${
              targetType === "person"
                ? "border-white/90 text-white"
                : "border-white/30 text-white/50 hover:text-white/80"
            }`}
          >
            Person
          </button>
        </div>
        <div className="input-wrapper relative">
          <div className="glass-bar">
            <textarea
              className="chat-input font-sans resize-none overflow-hidden h-auto min-h-[1.5em] max-h-[200px]"
              placeholder={targetType === "person" ? "Enter person name..." : "Enter company name and website..."}
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
            <div className="text-[10px] font-mono text-white/50 whitespace-nowrap ml-4 tracking-wider self-end mb-2">
              [Press Enter to Run]
            </div>
          </div>
        </div>

        {/* Line Data Metadata */}
        <div className="mt-4 font-mono text-xs text-white/60 flex flex-wrap items-center gap-2 cursor-pointer hover:text-white/80 transition-colors" onClick={() => setShowAdvanced(!showAdvanced)}>
          <span className="hover:underline decoration-white/50 underline-offset-4">
            {targetType === "person"
              ? (displayPersonName || "—")
              : (effectiveCompanyName || "—")}
          </span>
          {targetType === "person" && personLocation && (
            <>
              <span className="text-white/40">/</span>
              <span className="hover:underline decoration-white/50 underline-offset-4">
                {personLocation}
              </span>
            </>
          )}
          {effectiveWebsite && (
            <>
              <span className="text-white/40">/</span>
              <span className="hover:underline decoration-white/50 underline-offset-4">
                {effectiveWebsite}
              </span>
            </>
          )}
          <span className="text-white/40">/</span>
          <span className="text-[10px]">
            {prompt.length}/{MAX_CONTEXT_LENGTH}
          </span>
        </div>

        {/* Advanced Fields (Hidden by default) */}
        {showAdvanced && (
          <div className="mt-4 grid grid-cols-2 gap-4 pt-4 border-t border-white/20 font-mono text-xs">
            <div>
              <label className="block mb-2 text-white/60 uppercase tracking-wider text-[10px]">
                Company Name Override
              </label>
              <input
                type="text"
                className="w-full bg-transparent border-b border-white/30 py-1 text-white/90 focus:border-white/80 focus:outline-none transition-colors placeholder:text-white/30"
                placeholder="Inferred name..."
                value={overrideCompanyName}
                onChange={(e) => setOverrideCompanyName(e.target.value)}
              />
            </div>
            <div>
              <label className="block mb-2 text-white/60 uppercase tracking-wider text-[10px]">
                Website Override
              </label>
              <input
                type="text"
                className="w-full bg-transparent border-b border-white/30 py-1 text-white/90 focus:border-white/80 focus:outline-none transition-colors placeholder:text-white/30"
                placeholder="https://example.com"
                value={overrideWebsite}
                onChange={(e) => setOverrideWebsite(e.target.value)}
              />
            </div>
          </div>
        )}

        {/* Person fields - positioned absolutely to not affect centering */}
        <div 
          className={`absolute left-0 right-0 top-full mt-4 grid grid-cols-1 md:grid-cols-2 gap-4 pt-4 border-t border-white/20 font-mono text-xs transition-all duration-300 ${
            targetType === "person" 
              ? "opacity-100 translate-y-0 pointer-events-auto" 
              : "opacity-0 -translate-y-2 pointer-events-none"
          }`}
        >
            <div>
              <label className="block mb-2 text-white/60 uppercase tracking-wider text-[10px]">
                Affiliated Company (Optional)
              </label>
              <input
                type="text"
                className="w-full bg-transparent border-b border-white/30 py-1 text-white/90 focus:border-white/80 focus:outline-none transition-colors placeholder:text-white/30"
                placeholder="OpenAI"
                value={personAffiliation}
                onChange={(e) => setPersonAffiliation(e.target.value)}
                tabIndex={targetType === "person" ? 0 : -1}
              />
            </div>
            <div>
              <label className="block mb-2 text-white/60 uppercase tracking-wider text-[10px]">
                Location (Optional)
              </label>
              <input
                type="text"
                className="w-full bg-transparent border-b border-white/30 py-1 text-white/90 focus:border-white/80 focus:outline-none transition-colors placeholder:text-white/30"
                placeholder="San Francisco, CA"
                value={personLocation}
                onChange={(e) => setPersonLocation(e.target.value)}
                tabIndex={targetType === "person" ? 0 : -1}
              />
            </div>
          </div>

        {promptError && (
          <div className="mt-4 text-xs font-mono text-red-400 bg-red-900/10 py-2 px-3 border-l-2 border-red-500/50">
            {promptError}
          </div>
        )}
      </form>
    </div>
  );
}
