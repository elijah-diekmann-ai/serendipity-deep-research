import React from 'react';
import Link from 'next/link';

export default function ArchivePage() {
  return (
    <div className="max-w-4xl mx-auto py-10 px-4">
      <div className="mb-6">
        <Link href="/" className="text-sm text-gray-400 hover:text-white transition-colors">
          ← Back to Home
        </Link>
      </div>
      <h1 className="text-2xl font-light text-white mb-4">Research Archive</h1>
      <p className="text-gray-400 font-light">Previous research jobs will be listed here.</p>
    </div>
  );
}

