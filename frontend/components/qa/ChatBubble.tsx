import React from "react";
import ReactMarkdown from "react-markdown";
import { addCitationAnchors, getSafeHostname, Citation } from "../../lib/citations";

type ChatBubbleProps = {
  role: "user" | "assistant";
  markdown: string;
  createdAt?: string;
  citationIndexMap: Record<number, number>;
  citations: Citation[];
};

export function ChatBubble({
  role,
  markdown,
  createdAt,
  citationIndexMap,
  citations,
}: ChatBubbleProps) {
  const processedMarkdown = addCitationAnchors(markdown, citationIndexMap);

  return (
    <div className="relative">
      {/* Role indicator */}
      <div className="flex items-center gap-2 mb-2">
        <span className={`font-mono text-[9px] uppercase tracking-wider ${
          role === "user" ? "text-blue-600" : "text-gray-500"
        }`}>
          {role === "user" ? "query" : "response"}
        </span>
        {createdAt && (
          <>
            <span className="text-gray-300 font-mono text-[9px]">/</span>
            <span className="font-mono text-[9px] text-gray-400">
              {new Date(createdAt).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
            </span>
          </>
        )}
      </div>

      {/* Content */}
      <div
        className={`relative pl-3 ${
          role === "user"
            ? "border-l-2 border-blue-400"
            : "border-l border-gray-200"
        }`}
      >
        <div className={`prose prose-sm max-w-none 
          prose-p:leading-relaxed prose-p:text-gray-700 prose-p:my-2
          prose-ul:my-2 prose-ul:pl-4 prose-ul:text-gray-600
          prose-ol:my-2 prose-ol:pl-4 prose-ol:text-gray-600
          prose-li:my-0.5 prose-li:text-gray-600
          prose-strong:text-gray-900 prose-strong:font-medium
          prose-headings:text-gray-900 prose-headings:font-medium prose-headings:mt-4 prose-headings:mb-2
          prose-a:text-blue-600 prose-a:no-underline hover:prose-a:underline
          ${role === "user" ? "text-gray-800" : "text-gray-700"}
        `}>
          <ReactMarkdown
            components={{
              a({ href, children, ...props }) {
                if (href?.startsWith("#source-")) {
                  const id = Number(href.replace("#source-", ""));
                  const citation = citations.find((c) => c.id === id);
                  const title = citation?.title || citation?.url || "Source";
                  const domain = getSafeHostname(citation?.url) || citation?.provider;
                  
                  return (
                    <a
                      href={href}
                      title={domain ? `${title} – ${domain}` : title}
                      {...props}
                      className="inline-flex items-center font-mono text-[10px] text-blue-600 hover:text-blue-700 bg-blue-50 px-1.5 py-0.5 border border-blue-200 hover:border-blue-300 transition-colors"
                    >
                      {children}
                    </a>
                  );
                }
                return (
                  <a href={href} {...props} className="text-blue-600 hover:underline">
                    {children}
                  </a>
                );
              },
              // Custom styling for inline code
              code({ className, children, ...props }) {
                return (
                  <code 
                    className="font-mono text-[11px] bg-gray-100 border border-gray-200 px-1.5 py-0.5 text-gray-700"
                    {...props}
                  >
                    {children}
                  </code>
                );
              },
              // Style blockquotes
              blockquote({ children }) {
                return (
                  <blockquote className="border-l-2 border-gray-300 pl-3 my-3 text-gray-500 italic">
                    {children}
                  </blockquote>
                );
              },
            }}
          >
            {processedMarkdown}
          </ReactMarkdown>
        </div>
      </div>
    </div>
  );
}
