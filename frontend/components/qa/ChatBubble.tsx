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
    <div
      className={`bubble relative text-sm mb-4 ${
        role === "user"
          ? "bubble--user text-blue-900"
          : "bubble--assistant text-gray-800"
      }`}
    >
      <div className="prose prose-sm max-w-none prose-p:leading-normal prose-ul:pl-4 prose-a:text-blue-600 prose-a:no-underline hover:prose-a:underline">
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
                    className="text-blue-600 bg-blue-50 px-1 rounded font-mono text-xs hover:bg-blue-100 transition-colors"
                  >
                    {children}
                  </a>
                );
              }
              return (
                <a href={href} {...props}>
                  {children}
                </a>
              );
            },
          }}
        >
          {processedMarkdown}
        </ReactMarkdown>
      </div>
      {createdAt && (
        <div className={`text-[10px] mt-1 ${role === "user" ? "text-blue-900/60 text-right" : "text-gray-400"}`}>
          {new Date(createdAt).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
        </div>
      )}
    </div>
  );
}

