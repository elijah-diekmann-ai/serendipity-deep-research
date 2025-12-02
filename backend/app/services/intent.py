from typing import TypedDict, Literal, Optional, Dict, Any
import re
from urllib.parse import urlparse

class TargetIntent(TypedDict, total=False):
    target_type: Literal["company", "person", "unknown"]
    normalized_company_name: Optional[str]
    normalized_person_name: Optional[str]
    company_name: Optional[str]
    person_name: Optional[str]
    website: Optional[str]
    raw_query: Optional[str]
    country_code: Optional[str]
    # …other hints as needed

# Common company suffixes and keywords to avoid misclassifying as people
COMPANY_KEYWORDS = {
    "inc", "incorporated", "llc", "ltd", "limited", "corp", "corporation",
    "co", "company", "plc", "pty", "sa", "sarl", "bv", "nv", "ag", "gmbh",
    "labs", "capital", "systems", "tech", "group", "partners", "ventures",
    "holdings", "solutions", "global", "services", "management", "consulting",
    "associates", "advisors", "fund", "foundation", "institute", "software",
    "technologies", "studio", "studios", "media", "entertainment", "quantum",
    "network", "networks", "data", "research", "ai", "robotics", "bio",
    "pharma", "therapeutics", "health", "medical", "energy", "power", "legal",
    "finance", "investments", "logistics", "transport", "shipping", "mining",
    "resources", "bank", "insurance", "agency", "club"
}

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

def looks_like_person_name(value: str) -> bool:
    """
    Heuristic to check if a string looks like a person's name rather than a company.
    
    Rules:
    - 2-4 tokens.
    - Most tokens start with uppercase.
    - No company suffixes/keywords.
    - Not a URL.
    """
    if not value:
        return False
        
    if looks_like_url(value):
        return False
        
    s = value.strip()
    tokens = s.split()
    
    # Basic length check (First Name + Last Name, optionally Middle/Suffix)
    if not (2 <= len(tokens) <= 5):
        return False
        
    # Check for company keywords
    for token in tokens:
        # Strip punctuation for keyword check
        clean_token = re.sub(r"[^\w\s]", "", token).lower()
        if clean_token in COMPANY_KEYWORDS:
            return False
            
    # Check capitalization pattern (mostly title case)
    # Heuristic: At least 50% of alphabetic tokens should start with uppercase
    # RELAXED: If the input is all lowercase (common in search), we skip this check
    # provided it passes the keyword and length checks.
    alpha_tokens = [t for t in tokens if t and t[0].isalpha()]
    
    if not alpha_tokens:
        # e.g. "123 456"
        return False
        
    # If strictly lowercase, we allow it if it passes other checks (length, no keywords)
    is_all_lower = s.islower()
    
    if not is_all_lower:
        upper_count = sum(1 for t in alpha_tokens if t[0].isupper())
        if upper_count / len(alpha_tokens) < 0.5:
            return False
        
    # Ensure no digits in the tokens (avoid "John3" etc.)
    if any(any(char.isdigit() for char in token) for token in tokens):
        return False
    
    # Guard against obvious company-style suffixes in the final token
    tail = re.sub(r"[^\w]", "", tokens[-1]).lower()
    if tail in {
        "street",
        "road",
        "capital",
        "ventures",
        "partners",
        "group",
        "systems",
        "labs",
        "media",
    }:
        return False
        
    return True

def normalize_target_input(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    - Trim whitespace
    - If `website` is empty and `company_name` looks like a URL or domain,
      move it into `website`.
    - Populate `target_type`, `normalized_company_name`, and `normalized_person_name`.
    - Preserve original keys so existing planner logic still works.
    """
    # Create a copy to avoid mutating the original if it's used elsewhere
    normalized = dict(raw)
    
    raw_company_name = str(normalized.get("company_name") or "").strip()
    website = str(normalized.get("website") or "").strip()
    
    # Initial assignments
    company_name = raw_company_name
    person_name = str(normalized.get("person_name") or "").strip()
    location = str(normalized.get("location") or "").strip()
    
    # Heuristic: If company_name looks like a URL and website is empty, treat it as the website.
    if not website and looks_like_url(company_name):
        website = company_name

        # Attempt to extract a display name from the domain for better UX.
        try:
            if "://" not in website:
                parsed = urlparse(f"https://{website}")
            else:
                parsed = urlparse(website)
            
            if parsed.netloc:
                # e.g. www.example.com -> example
                parts = parsed.netloc.split('.')
                if len(parts) >= 2:
                    # Simple extraction of domain name
                    if parts[0] == "www":
                        company_name = parts[1]
                    else:
                        company_name = parts[0]
                    # capitalize
                    company_name = company_name.title()
        except Exception:
            pass
            
    # Determine target type
    requested_type = str(normalized.get("target_type") or "").strip().lower()
    explicit_type = requested_type in {"company", "person"}
    target_type: Literal["company", "person", "unknown"] = "company"
    if explicit_type:
        target_type = requested_type  # type: ignore[assignment]
    else:
        target_type = "company"
    
    if target_type == "person":
        if not person_name and company_name:
            # Preserve backwards compatibility: treat company_name as the person string
            person_name = company_name
            company_name = ""
    elif not explicit_type and looks_like_person_name(company_name):
        target_type = "person"
        person_name = company_name
        company_name = ""
    
    # Update normalized dict
    normalized["company_name"] = company_name or None
    normalized["person_name"] = person_name or None
    normalized["website"] = website or None
    normalized["target_type"] = target_type
    normalized["location"] = location or None
    
    # Build intent metadata
    intent_extra: TargetIntent = {
        "target_type": target_type,
        "normalized_company_name": company_name.lower().strip() if company_name else None,
        "normalized_person_name": person_name.lower().strip() if person_name else None,
        "company_name": company_name or None,
        "person_name": person_name or None,
        "website": website or None,
        "raw_query": raw.get("company_name"), # Store original input
        "country_code": raw.get("country_code"),
    }
    
    # Merge intent fields into the normalized dict
    normalized.update(intent_extra)
    
    return normalized
