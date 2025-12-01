from typing import TypedDict, Literal, Optional, Dict, Any
import re
from urllib.parse import urlparse

class TargetIntent(TypedDict, total=False):
    target_type: Literal["company", "person", "unknown"]
    normalized_company_name: Optional[str]
    website: Optional[str]
    raw_query: Optional[str]
    country_code: Optional[str]
    # …other hints as needed

def looks_like_url(value: str) -> bool:
    """
    Heuristic to check if a string looks like a URL or domain.
    """
    if not value:
        return False
    
    s = value.strip().lower()
    
    # Explicit protocol
    if s.startswith("http://") or s.startswith("https://"):
        return True
    
    # Starts with www.
    if s.startswith("www."):
        return True
        
    # Simple domain pattern: word.tld (where tld is 2+ chars)
    # e.g. "example.com", "google.co.uk"
    # Avoid matching simple filenames or abbreviations without dots
    if re.match(r"^[a-z0-9-]+\.[a-z]{2,}(\.[a-z]{2,})?$", s):
        return True
        
    return False

def normalize_target_input(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    - Trim whitespace
    - If `website` is empty and `company_name` looks like a URL or domain,
      move it into `website`.
    - Populate `target_type`, `normalized_company_name` where possible.
    - Preserve original keys so existing planner logic still works.
    """
    # Create a copy to avoid mutating the original if it's used elsewhere
    normalized = dict(raw)
    
    company_name = str(normalized.get("company_name") or "").strip()
    website = str(normalized.get("website") or "").strip()
    
    # Heuristic: if company_name looks like a URL and website is empty, swap/move it
    if not website and looks_like_url(company_name):
        website = company_name
        # If we moved it, we might want to clear company_name or try to extract a name from domain.
        # For now, let's clear it so we don't search for "www.google.com" as a company name string literallly
        # unless we want to extract the stem.
        # Let's keep it simple: if it's a URL, treat it as website. 
        # We can try to derive a name from the domain later or leave it empty 
        # (planner handles empty name if website exists).
        
        # Actually, let's try to extract a display name from the domain for better UX
        try:
            if "://" not in website:
                parsed = urlparse(f"https://{website}")
            else:
                parsed = urlparse(website)
            
            if parsed.netloc:
                # e.g. www.example.com -> example
                parts = parsed.netloc.split('.')
                if len(parts) >= 2:
                    # naive removal of www and tld
                    if parts[0] == "www":
                        company_name = parts[1]
                    else:
                        company_name = parts[0]
                    # capitalize
                    company_name = company_name.title()
        except Exception:
            pass
            
        normalized["website"] = website
        normalized["company_name"] = company_name

    # Update with trimmed values
    normalized["company_name"] = company_name
    normalized["website"] = website

    # Determine target type
    # Default to company for now as that's the main use case
    target_type: Literal["company", "person", "unknown"] = "company"
    
    # Future extension: detect person intent
    # if raw.get("person_name") or ...: target_type = "person"
    
    intent_extra: TargetIntent = {
        "target_type": target_type,
        "normalized_company_name": company_name if company_name else None,
        "website": website if website else None,
        "raw_query": raw.get("company_name"), # Store original input
        "country_code": raw.get("country_code"),
    }
    
    # Merge intent fields into the normalized dict (or keep them separate if preferred, 
    # but instructions say "Preserve original keys so existing planner logic still works")
    # We'll just ensure the base fields are clean.
    # The instruction says "Populate target_type... where possible".
    normalized.update(intent_extra)
    
    return normalized

