export const CITATION_LINK_REGEX = /\[S(\d+)\](?!\()/g;

export type Citation = {
  id: number;
  title?: string;
  url?: string;
  provider?: string;
};

export function buildCitationIndexMap(all: { id: number }[] = []) {
  const map: Record<number, number> = {};
  if (Array.isArray(all)) {
    all.forEach((c, i) => {
      if (c && typeof c.id === 'number') map[c.id] = i + 1;
    });
  }
  return map;
}

export function addCitationAnchors(markdown: string, map: Record<number, number>) {
  if (!markdown) return markdown;
  return markdown.replace(CITATION_LINK_REGEX, (_m, id) => {
    const sid = Number(id);
    const short = map[sid];
    return `[${short ? `S${short}` : `S${sid}`}](#source-${sid})`;
  });
}

export function getSafeHostname(url?: string) {
  if (!url) return null;
  try {
    return new URL(url).hostname;
  } catch {
    return null;
  }
}

