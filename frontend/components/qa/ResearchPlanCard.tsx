"use client";

import { ResearchPlanProposal } from "../../lib/api";
import ReactMarkdown from "react-markdown";

type ResearchPlanCardProps = {
  plan: ResearchPlanProposal;
  onRun: () => void;
  loading?: boolean;
  disabled?: boolean;
};

/**
 * Renders a micro-research plan proposal card within the Q&A chat.
 * Terminal-style design matching the query interface aesthetic.
 */
export function ResearchPlanCard({
  plan,
  onRun,
  loading = false,
  disabled = false,
}: ResearchPlanCardProps) {
  const costLabel = plan.estimated_cost?.label || "small";

  return (
    <div className="border border-gray-200 bg-white">
      {/* Header */}
      <div className="px-4 py-3 border-b border-gray-100 flex items-center justify-between bg-gray-50/50">
        <div className="flex items-center gap-3">
          <div className="w-1.5 h-1.5 rounded-full bg-amber-500" />
          <span className="font-mono text-[10px] uppercase tracking-wider text-gray-600">
            Additional Research Available
          </span>
        </div>
        <span className="font-mono text-[9px] uppercase tracking-wider text-gray-400">
          [{costLabel}]
        </span>
      </div>

      {/* Content */}
      <div className="px-4 py-3 space-y-3">
        {/* Gap Statement */}
        <div className="flex items-start gap-3">
          <span className="font-mono text-[9px] text-amber-600 mt-0.5 shrink-0">GAP:</span>
          <p className="font-mono text-[11px] text-gray-600 leading-relaxed">
            {plan.gap_statement}
          </p>
        </div>

        {/* Plan Markdown */}
        {plan.plan_markdown && (
          <div className="border-l border-gray-200 pl-3 mt-3">
            <div className="font-mono text-[9px] uppercase tracking-wider text-gray-400 mb-2">
              Proposed Steps
            </div>
            <div className="prose prose-sm max-w-none 
              prose-p:text-gray-500 prose-p:text-[11px] prose-p:font-mono prose-p:my-1 prose-p:leading-relaxed
              prose-strong:text-gray-700 prose-strong:font-medium
              prose-ol:my-1 prose-ol:pl-4 prose-ol:text-gray-500
              prose-ul:my-1 prose-ul:pl-4 prose-ul:text-gray-500
              prose-li:text-[11px] prose-li:font-mono prose-li:my-0.5
            ">
              <ReactMarkdown
                components={{
                  p: ({ children }) => <p>{children}</p>,
                  strong: ({ children }) => <strong>{children}</strong>,
                  ol: ({ children }) => <ol className="list-decimal">{children}</ol>,
                  ul: ({ children }) => <ul className="list-disc">{children}</ul>,
                  li: ({ children }) => <li>{children}</li>,
                }}
              >
                {plan.plan_markdown}
              </ReactMarkdown>
            </div>
          </div>
        )}

        {/* Action Button */}
        <div className="flex items-center justify-end pt-2">
          <button
            onClick={onRun}
            disabled={loading || disabled}
            className={`
              flex items-center gap-2 px-4 py-2 font-mono text-[10px] uppercase tracking-wider
              border transition-all
              ${loading || disabled
                ? "border-gray-200 text-gray-300 cursor-not-allowed bg-gray-50"
                : "border-gray-300 text-gray-600 hover:border-gray-400 hover:text-gray-900 hover:bg-gray-50"
              }
            `}
          >
            {loading ? (
              <>
                <span className="flex space-x-1">
                  <span className="w-1 h-1 bg-gray-400 rounded-full animate-pulse" />
                  <span className="w-1 h-1 bg-gray-400 rounded-full animate-pulse" style={{ animationDelay: "150ms" }} />
                  <span className="w-1 h-1 bg-gray-400 rounded-full animate-pulse" style={{ animationDelay: "300ms" }} />
                </span>
                <span>Running...</span>
              </>
            ) : (
              <>
                <span>▶</span>
                <span>Run Research</span>
              </>
            )}
          </button>
        </div>
      </div>
    </div>
  );
}
