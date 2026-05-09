#!/usr/bin/env python3
"""
Drugged App - Altibbi Description Enrichment Pipeline
======================================================

A complete 7-step pipeline to enrich drug data from Altibbi.com using
DuckDuckGo HTML search (free, unlimited, no API key required).

Output: SQLite database with enriched descriptions, fully compatible
with existing Drugged App ecosystem.

Usage:
    python altibbi_enrichment_pipeline.py --input drugged.db \
            --output drugged_enriched.db
    python altibbi_enrichment_pipeline.py --test  # Test with 5 drugs
    python altibbi_enrichment_pipeline.py --category ANTIBIOTIC  \
        # Process only antibiotics
    python altibbi_enrichment_pipeline.py --export-csv  # Export to CSV
    python altibbi_enrichment_pipeline.py --merge  # Merge back to original DB

Author: Generated for Drugged App
Date: 2026-05-09
"""

import sqlite3
import re
import time
import random
import urllib.request
import urllib.parse
import urllib.error
import ssl
import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List
import html

# ============================================================================
# CONFIGURATION
# ============================================================================


class Config:
    """Pipeline configuration"""

    # Database
    INPUT_DB = "drugged.db"  # Your existing SQLite file
    OUTPUT_DB = "drugged_enriched.db"  # New enriched database

    # Search
    SEARCH_ENGINE = "duckduckgo_html"  # Free, unlimited, no API key
    SITE_FILTER = "altibbi.com"
    MAX_RESULTS_PER_QUERY = 3  # Top N results to evaluate

    # Throttling (CRITICAL - prevents blocking)
    MIN_DELAY = 2.0  # Minimum seconds between requests
    MAX_DELAY = 5.0  # Maximum seconds between requests
    JITTER = 1.5  # Random jitter +/- seconds
    TIMEOUT = 15  # Request timeout seconds
    MAX_RETRIES = 3  # Retries on failure
    RETRY_BACKOFF = 60  # Seconds to wait after 429/403

    # Batch processing
    BATCH_SIZE = 100  # Process N drugs per batch
    DAILY_CAP = 3000  # Max requests per day
    CHECKPOINT_INTERVAL = 50  # Save progress every N drugs

    # Output
    LOG_FILE = "enrichment_log.txt"

    # User Agents for rotation
    USER_AGENTS = [
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/119.0.0.0 Safari/537.36"
        ),
        (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/118.0.0.0 Safari/537.36"
        ),
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) "
            "Gecko/20100101 Firefox/121.0"
        ),
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) "
            "Version/17.1 Safari/605.1.15"
        ),
    ]


# ============================================================================
# STEP 1: NAME NORMALIZATION
# ============================================================================


class NameNormalizer:
    """
    Clean drug trade names for optimal search matching.

    Problem: DB names contain dosage/form info that breaks Altibbi search
    Solution: Strip all dosage/form suffixes, keep only base product name
    """

    # Patterns to remove (order matters - most specific first)
    STRIP_PATTERNS = [
        # Dosage forms with counts
        r"\s+\d+\s*F\.C\..*",  # " 10 F.C. TABS"
        r"\s+\d+\s*F\.C\.T.*",  # " 10 F.C.TABS"
        r"\s+\d+\s*S\.F\.C\..*",  # " 5 S.F.C.TABS"
        r"\s+\d+\s*E\.C\..*",  # " 30 E.C.TABS"
        r"\s+\d+\s*S\.R\..*",  # " 20 S.R. CAPS"
        r"\s+\d+\s*X\.R\..*",  # " 30 X.R. TABS"
        r"\s+\d+\s*X\.T\..*",  # " 30 X.T. TABS"
        r"\s+\d+\s*S\.C\..*",  # " 5 S.C. AMPS"
        r"\s+\d+\s*I\.M\..*",  # " 5 I.M. AMPS"
        r"\s+\d+\s*I\.V\..*",  # " 1 I.V. VIAL"
        # Standard forms
        r"\s+\d+\s*TABS?.*",  # " 10 TABS" / " 10 TABLETS"
        r"\s+\d+\s*CAPS?.*",  # " 10 CAPS" / " 10 CAPSULES"
        r"\s+\d+\s*VIAL.*",  # " 5 VIALS"
        r"\s+\d+\s*AMP.*",  # " 5 AMPS" / " 5 AMPULES"
        r"\s+\d+\s*SACHET.*",  # " 10 SACHETS"
        r"\s+\d+\s*SUPP.*",  # " 5 SUPP"
        r"\s+\d+\s*PD\..*",  # " PD. FOR ORAL SUSP"
        r"\s+\d+\s*SSb\..*",  # " 60ML SSb."
        r"\s+\d+\s*SRb\..*",  # " 120ML SRb."
        r"\s+\d+\s*Mb\..*",  # " 5 Mb."
        r"\s+\d+\s*DRbS.*",  # " 30 DRbS"
        r"\s+\d+\s*SkTS.*",  # " 10 SkTS"
        r"\s+\d+\s*SkRD.*",  # " 30 SkRD TBS"
        r"\s+\d+\s*Sbb\..*",  # " 5 Sbb."
        r"\s+\d+\s*F\.k\..*",  # " 10 F.k. TBS"
        r"\s+\d+\s*F\.k\.T.*",  # " 10 F.k.TABS"
        r"\s+\d+\s*kbS.*",  # " 30 kbS" / " 30 kbSLS"
        r"\s+\d+\s*kbSLS.*",  # " 30 kbSLS"
        r"\s+\d+\s*bTk.*",  # " 12 bTk"
        r"\s+\d+\s*bRF.*",  # " 4 bRF. SRNGS"
        r"\s+\d+\s*SRNG.*",  # " 4 SRNGS"
        r"\s+\d+\s*bsS.*",  # " 20 bsS"
        r"\s+\d+\s*bx.*",  # " 50 bx"
        r"\s+\d+\s*bWDR.*",  # " 510 GRM bWDR"
        r"\s+\d+\s*bD\..*",  # " bD. FR RL SSb"
        r"\s+\d+\s*GM.*",  # " 50 GM" / " 50 GRM"
        r"\s+\d+\s*GRM.*",  # " 50 GRM"
        r"\s+\d+\s*ML.*",  # " 120 ML"
        r"\s+\d+\s*L.*",  # " 1 L"
        # Dosage strengths
        r"\s+\d+\s*MG.*",  # " 500 MG"
        r"\s+\d+\s*MCG.*",  # " 400 MCG"
        r"\s+\d+\s*IU.*",  # " 2000 IU"
        r"\s+\d+\s*U\..*",  # " 100 U."
        r"\s+\d+\s*UNIT.*",  # " 1000 UNITS"
        r"\s+\d+\s*GM/\d+.*",  # " 500MG/5ML"
        r"\s+\d+\s*MG/\d+.*",  # " 250MG/5ML"
        r"\s+\d+/\d+\s*MG.*",  # " 500/65MG"
        r"\s+\d+\.\d+\s*MG.*",  # " 2.5 MG"
        r"\s+\d+\s*%.*",  # " 0.05%"
        r"\s+\d+\.\d+\s*%.*",  # " 0.025%"
        # Form descriptors
        r"\s+SYRUP.*",  # " SYRUP"
        r"\s+SUSP\..*",  # " SUSP."
        r"\s+SOLUTION.*",  # " SOLUTION"
        r"\s+SOLN\..*",  # " SOLN."
        r"\s+CREAM.*",  # " CREAM"
        r"\s+GEL.*",  # " GEL"
        r"\s+OINTMENT.*",  # " OINTMENT"
        r"\s+NTMNT.*",  # " NTMNT"
        r"\s+LOTION.*",  # " LOTION"
        r"\s+LTN.*",  # " LTN"
        r"\s+DROPS.*",  # " DROPS"
        r"\s+DRbS.*",  # " DRbS"
        r"\s+SPRAY.*",  # " SPRAY"
        r"\s+SbR.*",  # " SbR"
        r"\s+SOAP.*",  # " SOAP"
        r"\s+Sb.*",  # " Sb"
        r"\s+Wk.*",  # " Wk"
        r"\s+WASH.*",  # " WASH"
        r"\s+Ms Wk.*",  # " Ms Wk"
        r"\s+Ms SbR.*",  # " Ms SbR"
        r"\s+GL.*",  # " GL"
        r"\s+SRM.*",  # " SRM"
        r"\s+SERUM.*",  # " SERUM"
        r"\s+MSK.*",  # " MSK"
        r"\s+kMb.*",  # " kMb" (shampoo)
        r"\s+kRM.*",  # " kRM" (cream)
        r"\s+kLNSR.*",  # " kLNSR"
        r"\s+kLNSNG.*",  # " kLNSNG"
        r"\s+SLTN.*",  # " SLTN"
        r"\s+SNDT.*",  # " SNDT"
        r"\s+TRTMNT.*",  # " TRTMNT"
        r"\s+TRT.*",  # " TRT"
        r"\s+INHAL.*",  # " INHALER"
        r"\s+NHLR.*",  # " NHLR"
        r"\s+NBLsR.*",  # " NBLsR"
        r"\s+RSbRTR.*",  # " RSbRTR"
        r"\s+NFSN.*",  # " NFSN"
        r"\s+INF.*",  # " INFUSION"
        r"\s+fG\.Dk.*",  # " fG.Dk" (douche)
        r"\s+fL.*",  # " fL" (vial)
        r"\s+RL.*",  # " RL" (real)
        r"\s+FR.*",  # " FR" (for)
        # Route descriptors
        r"\s+ORAL\..*",  # " ORAL.SOLID"
        r"\s+TOPICAL.*",  # " TOPICAL"
        r"\s+TbkL.*",  # " TbkL"
        r"\s+INJECTION.*",  # " INJECTION"
        r"\s+RECTAL.*",  # " RECTAL"
        r"\s+VAGINAL.*",  # " VAGINAL"
        r"\s+NASAL.*",  # " NASAL"
        r"\s+EYE.*",  # " EYE"
        r"\s+EAR.*",  # " EAR"
        r"\s+MOUTH.*",  # " MOUTH"
        r"\s+EFF.*",  # " EFF"
        # Parentheticals and misc
        r"\s*\(.*\).*",  # " (N/A)" / " (ILLEGAL IMPORT)"
        r"\s*\[.*\].*",  # " [CANCELLED]"
        r"\s*\{.*\}.*",  # " {anything}"
        r"\s+CANCELLED.*",  # " CANCELLED"
        r"\s+ILLEGAL IMPORT.*",  # " ILLEGAL IMPORT"
        r"\s+N/A.*",  # " N/A"
        r"\s+NEW.*",  # " NEW"
        r"\s+EXTRA.*",  # " EXTRA"
        r"\s+FORTE.*",  # " FORTE"
        r"\s+PLUS.*",  # " PLUS"
        r"\s+ADVANCED.*",  # " ADVANCED"
        r"\s+ORIGINAL.*",  # " ORIGINAL"
        r"\s+LIGHT.*",  # " LIGHT"
        r"\s+MAX.*",  # " MAX"
        r"\s+ULTRA.*",  # " ULTRA"
        r"\s+SUPER.*",  # " SUPER"
        r"\s+PRO.*",  # " PRO"
        r"\s+ACT.*",  # " ACT"
        r"\s+RAPID.*",  # " RAPID"
        r"\s+FAST.*",  # " FAST"
        r"\s+SLOW.*",  # " SLOW"
        r"\s+RETARD.*",  # " RETARD"
        r"\s+DELAYED.*",  # " DELAYED"
        r"\s+EXTENDED.*",  # " EXTENDED"
        r"\s+CONTROLLED.*",  # " CONTROLLED"
        r"\s+MODIFIED.*",  # " MODIFIED"
        r"\s+LONG.*",  # " LONG"
        r"\s+SHORT.*",  # " SHORT"
        r"\s+IMMEDIATE.*",  # " IMMEDIATE"
        r"\s+SUSTAINED.*",  # " SUSTAINED"
        r"\s+PROLONGED.*",  # " PROLONGED"
        r"\s+CONTINUOUS.*",  # " CONTINUOUS"
        r"\s+INTERMITTENT.*",  # " INTERMITTENT"
        r"\s+PULSE.*",  # " PULSE"
        r"\s+BOLUS.*",  # " BOLUS"
        r"\s+INFUSION.*",  # " INFUSION"
        r"\s+NEBULIZER.*",  # " NEBULIZER"
        r"\s+NEB.*",  # " NEB"
        r"\s+INHALER.*",  # " INHALER"
        r"\s+PUFF.*",  # " PUFF"
        r"\s+DOSE.*",  # " DOSE"
        r"\s+DOSES.*",  # " DOSES"
        r"\s+MONO-DOSE.*",  # " MONO-DOSE"
        r"\s+MULTI-DOSE.*",  # " MULTI-DOSE"
        r"\s+SINGLE.*",  # " SINGLE"
        r"\s+DOUBLE.*",  # " DOUBLE"
        r"\s+TRIPLE.*",  # " TRIPLE"
        r"\s+QUAD.*",  # " QUAD"
        r"\s+QUINT.*",  # " QUINT"
        r"\s+HEXA.*",  # " HEXA"
        r"\s+OCTA.*",  # " OCTA"
        r"\s+DECA.*",  # " DECA"
        r"\s+MULTI.*",  # " MULTI"
        r"\s+UNI.*",  # " UNI"
        r"\s+BI.*",  # " BI"
        r"\s+TRI.*",  # " TRI"
        r"\s+QUADRI.*",  # " QUADRI"
        r"\s+PENTA.*",  # " PENTA"
        r"\s+HEXA.*",  # " HEXA"
        r"\s+HEPTA.*",  # " HEPTA"
        r"\s+OCTA.*",  # " OCTA"
        r"\s+NONA.*",  # " NONA"
        r"\s+DECA.*",  # " DECA"
        r"\s+POLY.*",  # " POLY"
        r"\s+OLIGO.*",  # " OLIGO"
        r"\s+MONO.*",  # " MONO"
        r"\s+DI.*",  # " DI"
        r"\s+TRI.*",  # " TRI"
        r"\s+TETRA.*",  # " TETRA"
        r"\s+PENTA.*",  # " PENTA"
        r"\s+HEXA.*",  # " HEXA"
        r"\s+HEPTA.*",  # " HEPTA"
        r"\s+OCTA.*",  # " OCTA"
        r"\s+NONA.*",  # " NONA"
        r"\s+DECA.*",  # " DECA"
        r"\s+UNDECA.*",  # " UNDECA"
        r"\s+DODECA.*",  # " DODECA"
        r"\s+ICOSA.*",  # " ICOSA"
        r"\s+HENICOSA.*",  # " HENICOSA"
        r"\s+DOKOSA.*",  # " DOKOSA"
        r"\s+TRICOSA.*",  # " TRICOSA"
        r"\s+TETRAKOSA.*",  # " TETRAKOSA"
        r"\s+PENTAKOSA.*",  # " PENTAKOSA"
        r"\s+HEXAKOSA.*",  # " HEXAKOSA"
        r"\s+HEPTAKOSA.*",  # " HEPTAKOSA"
        r"\s+OCTAKOSA.*",  # " OCTAKOSA"
        r"\s+NONAKOSA.*",  # " NONAKOSA"
        r"\s+TRIACONTA.*",  # " TRIACONTA"
        r"\s+TETRACONTA.*",  # " TETRACONTA"
        r"\s+PENTACONTA.*",  # " PENTACONTA"
        r"\s+HEXACONTA.*",  # " HEXACONTA"
        r"\s+HEPTACONTA.*",  # " HEPTACONTA"
        r"\s+OCTACONTA.*",  # " OCTACONTA"
        r"\s+NONACONTA.*",  # " NONACONTA"
        r"\s+HECTA.*",  # " HECTA"
        r"\s+CHILIA.*",  # " CHILIA"
        r"\s+MYRIA.*",  # " MYRIA"
        r"\s+MEGA.*",  # " MEGA"
        r"\s+GIGA.*",  # " GIGA"
        r"\s+TERA.*",  # " TERA"
        r"\s+PETA.*",  # " PETA"
        r"\s+EXA.*",  # " EXA"
        r"\s+ZETTA.*",  # " ZETTA"
        r"\s+YOTTA.*",  # " YOTTA"
        r"\s+RONNA.*",  # " RONNA"
        r"\s+QUETTA.*",  # " QUETTA"
        # Numbers at end (standalone)
        r"\s+\d+$",  # trailing number
        r"\s+\d+\.\d+$",  # trailing decimal
        # Cleanup
        r"\s+-\s*$",  # trailing dash
        r"\s+\.\s*$",  # trailing dot
        r"\s*,\s*$",  # trailing comma
        r"\s*;\s*$",  # trailing semicolon
        r"\s*:\s*$",  # trailing colon
        r"\s*\|\s*$",  # trailing pipe
        r"\s*/\s*$",  # trailing slash
        r"\s*\\\s*$",  # trailing backslash
        r"\s*\(\s*$",  # trailing open paren
        r"\s*\[\s*$",  # trailing open bracket
        r"\s*\{\s*$",  # trailing open brace
        r"\s*<\s*$",  # trailing open angle
        r"\s*>\s*$",  # trailing close angle
        r"\s*&\s*$",  # trailing ampersand
        r"\s*%\s*$",  # trailing percent
        r"\s*\$\s*$",  # trailing dollar
        r"\s*#\s*$",  # trailing hash
        r"\s*@\s*$",  # trailing at
        r"\s*!\s*$",  # trailing exclamation
        r"\s*\?\s*$",  # trailing question
        r"\s*\+\s*$",  # trailing plus
        r"\s*\*\s*$",  # trailing asterisk
        r"\s*=\s*$",  # trailing equals
        r"\s*~\s*$",  # trailing tilde
        r"\s*`\s*$",  # trailing backtick
        r"\s*\^\s*$",  # trailing caret
        r"\s*\.\.\..*",  # ellipsis and after
    ]

    # Compile patterns once
    COMPILED_PATTERNS = [re.compile(p, re.IGNORECASE) for p in STRIP_PATTERNS]

    @classmethod
    def normalize(cls, name: str) -> str:
        """
        Normalize a drug trade name for search.

        Args:
            name: Raw trade_name from database

        Returns:
            Clean base name suitable for Altibbi search
        """
        if not name or not isinstance(name, str):
            return ""

        # Start with original
        clean = name.strip()

        # Skip placeholder entries (all zeros, all nines, very short)
        if re.match(r"^[09]+$", clean) or len(clean) < 3:
            return ""

        # Apply all strip patterns
        for pattern in cls.COMPILED_PATTERNS:
            clean = pattern.sub("", clean)

        # Final cleanup
        clean = clean.strip()
        clean = re.sub(r"\s+", " ", clean)  # Normalize whitespace
        clean = re.sub(r"\s*[-_/\|]+\s*", " ", clean)  # Remove separator chars
        clean = re.sub(r"\s+", " ", clean)  # Normalize again

        # Remove trailing non-word chars
        clean = re.sub(r"[^\w\s]+$", "", clean)

        return clean.strip()

    @classmethod
    def generate_search_queries(
        cls, trade_name: str, active_ingredient: Optional[str] = None
    ) -> List[str]:
        """
        Generate multiple search query variations for maximum match
        probability.

        Returns list of queries in priority order (most specific first).
        """
        queries = []
        base = cls.normalize(trade_name)

        if not base:
            return queries

        # Primary: Exact trade name on Altibbi
        queries.append(f"{base} site:altibbi.com")

        # Secondary: Trade name + "duwa" (medicine in Arabic transliteration)
        queries.append(f"{base} altibbi medicine")

        # Tertiary: If base has spaces, try first word (brand name only)
        parts = base.split()
        if len(parts) > 1 and len(parts[0]) > 2:
            queries.append(f"{parts[0]} site:altibbi.com")

        # Quaternary: Active ingredient search (if available)
        if active_ingredient:
            # Take first active ingredient only
            first_active = active_ingredient.split("+")[0].strip()
            if len(first_active) > 2:
                queries.append(f"{first_active} site:altibbi.com")

        return queries


# ============================================================================
# STEP 2: DUCKDUCKGO HTML SEARCH
# ============================================================================


class DuckDuckGoSearcher:
    """
    Free, unlimited DuckDuckGo HTML search.
    No API key required. No rate limits (with polite throttling).
    """

    def __init__(self):
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
        self.last_request_time = 0
        self.request_count = 0
        self.daily_count = 0
        self.session_start = datetime.now()

    def _get_headers(self) -> dict:
        """Rotate user agents and set realistic headers."""
        ua = random.choice(Config.USER_AGENTS)
        return {
            "User-Agent": ua,
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/webp,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.5,ar;q=0.3",
            "Accept-Encoding": "identity",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Cache-Control": "max-age=0",
        }

    def _throttle(self):
        """Apply intelligent throttling with jitter."""
        elapsed = time.time() - self.last_request_time
        delay = random.uniform(Config.MIN_DELAY, Config.MAX_DELAY)
        jitter = random.uniform(-Config.JITTER, Config.JITTER)
        total_delay = max(0, delay + jitter - elapsed)

        if total_delay > 0:
            time.sleep(total_delay)

        self.last_request_time = time.time()

    def _check_daily_cap(self) -> bool:
        """Check if we've hit the daily request cap."""
        today = datetime.now().date()
        session_date = self.session_start.date()

        if today != session_date:
            self.daily_count = 0
            self.session_start = datetime.now()

        return self.daily_count < Config.DAILY_CAP

    def search(self, query: str, retries: int = 0) -> List[Dict[str, str]]:
        """
        Execute DuckDuckGo HTML search.

        Args:
            query: Search query string
            retries: Current retry attempt

        Returns:
            List of result dicts with 'title', 'url', 'snippet'
        """
        if not self._check_daily_cap():
            print(
                f"[WARNING] Daily cap reached ({Config.DAILY_CAP}). Stopping."
            )
            return []

        self._throttle()

        encoded_query = urllib.parse.quote(query)
        url = f"https://html.duckduckgo.com/html/?q={encoded_query}"

        req = urllib.request.Request(url, headers=self._get_headers())

        try:
            with urllib.request.urlopen(
                req, context=self.ssl_context, timeout=Config.TIMEOUT
            ) as response:
                html_content = response.read().decode("utf-8", errors="ignore")
                self.request_count += 1
                self.daily_count += 1
                return self._parse_results(html_content)

        except urllib.error.HTTPError as e:
            if e.code == 429 and retries < Config.MAX_RETRIES:
                print(f"[429 Too Many] Backing off {Config.RETRY_BACKOFF}s...")
                time.sleep(Config.RETRY_BACKOFF)
                return self.search(query, retries + 1)
            elif e.code == 403 and retries < Config.MAX_RETRIES:
                print("[403 Forbidden] Retrying with new UA...")
                time.sleep(Config.RETRY_BACKOFF / 2)
                return self.search(query, retries + 1)
            else:
                print(f"[HTTP ERROR {e.code}] {e.reason}")
                return []

        except urllib.error.URLError as e:
            if retries < Config.MAX_RETRIES:
                print(
                    f"[URL Error] Retrying... "
                    f"({retries+1}/{Config.MAX_RETRIES})"
                )
                time.sleep(5)
                return self.search(query, retries + 1)
            else:
                print(f"[URL ERROR] {e.reason}")
                return []

        except Exception as e:
            print(f"[ERROR] {type(e).__name__}: {e}")
            return []

    def _parse_results(self, html_content: str) -> List[Dict[str, str]]:
        """Parse DuckDuckGo HTML results."""
        results = []

        # Pattern 1: Standard result links (result__a class)
        pattern1 = re.findall(
            r'<a[^>]*class="result__a"[^>]*href="([^"]*)"[^>]*>(.*?)</a>',
            html_content,
            re.DOTALL,
        )

        # Pattern 2: Alternative result format
        pattern2 = re.findall(
            r'<a[^>]*class="result__snippet"[^>]*href="([^"]*)"[^>]*>(.*?)</a>',
            html_content,
            re.DOTALL,
        )

        # Pattern 3: Generic link with result class
        pattern3 = re.findall(
            r'<a[^>]*href="([^"]*result[^"]*)"[^>]*>(.*?)</a>',
            html_content,
            re.DOTALL,
        )

        all_patterns = pattern1 + pattern2 + pattern3

        for url, title_html in all_patterns[: Config.MAX_RESULTS_PER_QUERY]:
            # Clean title
            title = re.sub(r"<[^>]+>", "", title_html)
            title = html.unescape(title).strip()

            # Clean URL (DuckDuckGo uses redirects)
            clean_url = self._extract_real_url(url)

            if clean_url and "altibbi.com" in clean_url:
                results.append(
                    {
                        "title": title,
                        "url": clean_url,
                        "snippet": "",  # Will be filled if we scrape the page
                    }
                )

        return results

    def _extract_real_url(self, duck_url: str) -> str:
        """Extract real URL from DuckDuckGo redirect URL."""
        # DuckDuckGo URLs look like: /l/?kh=-1&uddg=https%3A%2F%2F...
        if "uddg=" in duck_url:
            match = re.search(r"uddg=([^&]+)", duck_url)
            if match:
                return urllib.parse.unquote(match.group(1))

        # Direct URL
        if duck_url.startswith("http"):
            return duck_url

        # Relative URL
        if duck_url.startswith("/"):
            return f"https://duckduckgo.com{duck_url}"

        return duck_url


# ============================================================================
# STEP 3: ALTIBBI PAGE SCRAPER
# ============================================================================


class AltibbiScraper:
    """
    Scrape medicine descriptions from Altibbi.com drug pages.
    Extracts: title, meta description, main description, uses, side effects.
    """

    def __init__(self):
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
        self.last_request_time = 0

    def _throttle(self):
        """Throttle between page requests."""
        elapsed = time.time() - self.last_request_time
        delay = random.uniform(1.5, 3.5)
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self.last_request_time = time.time()

    def _get_headers(self) -> dict:
        return {
            "User-Agent": random.choice(Config.USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ar,en-US;q=0.7,en;q=0.3",
            "Accept-Encoding": "identity",
            "Connection": "keep-alive",
        }

    def scrape(self, url: str, retries: int = 0) -> Optional[Dict[str, str]]:
        """
        Scrape a single Altibbi drug page.

        Args:
            url: Full Altibbi URL (e.g., https://altibbi.com/الادوية/بانادول)

        Returns:
            Dict with extracted fields or None if failed
        """
        self._throttle()

        req = urllib.request.Request(url, headers=self._get_headers())

        try:
            with urllib.request.urlopen(
                req, context=self.ssl_context, timeout=Config.TIMEOUT
            ) as response:
                html_content = response.read().decode("utf-8", errors="ignore")
                return self._parse_page(html_content, url)

        except urllib.error.HTTPError as e:
            if e.code == 429 and retries < Config.MAX_RETRIES:
                time.sleep(Config.RETRY_BACKOFF)
                return self.scrape(url, retries + 1)
            print(f"[HTTP {e.code}] {url}")
            return None

        except Exception as e:
            print(f"[ERROR scraping {url}] {e}")
            return None

    def _parse_page(self, html_content: str, url: str) -> Dict[str, str]:
        """Extract structured data from Altibbi HTML."""
        data = {
            "source_url": url,
            "page_title": "",
            "meta_description": "",
            "main_description": "",
            "uses": "",
            "side_effects": "",
            "contraindications": "",
            "precautions": "",
            "dosage": "",
            "storage": "",
            "scraped_at": datetime.now().isoformat(),
        }

        # 1. Page Title
        title_match = re.search(
            r"<title[^>]*>(.*?)</title>", html_content, re.DOTALL
        )
        if title_match:
            data["page_title"] = html.unescape(
                re.sub(r"<[^>]+>", "", title_match.group(1))
            ).strip()

        # 2. Meta Description
        meta_match = re.search(
            r'<meta[^>]*name=["\']description["\'][^>]*content=["\']([^"\']*)["\']',
            html_content,
            re.IGNORECASE,
        )
        if not meta_match:
            meta_match = re.search(
                r'<meta[^>]*content=["\']([^"\']*)["\'][^>]*name=["\']description["\']',
                html_content,
                re.IGNORECASE,
            )
        if meta_match:
            data["meta_description"] = html.unescape(
                meta_match.group(1)
            ).strip()

        # 3. Main Description ("ma huwa dawa" section)
        desc_patterns = [
            r"ما\s+هو\s+دواء[\s\w]*?<p[^>]*>(.*?)</p>",
            r"ما\s+هو[\s\w]*?<p[^>]*>(.*?)</p>",
            r"تعريف[\s\w]*?<p[^>]*>(.*?)</p>",
            r"نبذة[\s\w]*?<p[^>]*>(.*?)</p>",
        ]
        for pattern in desc_patterns:
            match = re.search(pattern, html_content, re.DOTALL | re.IGNORECASE)
            if match:
                data["main_description"] = self._clean_html(match.group(1))
                break

        # 4. Uses ("استخدامات" section)
        uses_patterns = [
            r"استخدامات[\s\w]*?<ul[^>]*>(.*?)</ul>",
            r"دواعي[\s\w]*?<ul[^>]*>(.*?)</ul>",
            r"uses[\s\w]*?<ul[^>]*>(.*?)</ul>",
        ]
        for pattern in uses_patterns:
            match = re.search(pattern, html_content, re.DOTALL | re.IGNORECASE)
            if match:
                data["uses"] = self._extract_list_items(match.group(1))
                break

        # 5. Side Effects ("al-a'rād al-jānibīyah" section)
        side_patterns = [
            r"الاعراض[\s\w]*?<ul[^>]*>(.*?)</ul>",
            r"اعراض[\s\w]*?<ul[^>]*>(.*?)</ul>",
            r"side\s+effects[\s\w]*?<ul[^>]*>(.*?)</ul>",
        ]
        for pattern in side_patterns:
            match = re.search(pattern, html_content, re.DOTALL | re.IGNORECASE)
            if match:
                data["side_effects"] = self._extract_list_items(match.group(1))
                break

        # 6. Contraindications ("mawāni' al-istikhdām" section)
        contra_patterns = [
            r"موانع[\s\w]*?<ul[^>]*>(.*?)</ul>",
            r"contraindications[\s\w]*?<ul[^>]*>(.*?)</ul>",
        ]
        for pattern in contra_patterns:
            match = re.search(pattern, html_content, re.DOTALL | re.IGNORECASE)
            if match:
                data["contraindications"] = self._extract_list_items(
                    match.group(1)
                )
                break

        # 7. Precautions ("taḥdhīrāt" section)
        prec_patterns = [
            r"تحذيرات[\s\w]*?<ul[^>]*>(.*?)</ul>",
            r"احتياطات[\s\w]*?<ul[^>]*>(.*?)</ul>",
            r"precautions[\s\w]*?<ul[^>]*>(.*?)</ul>",
        ]
        for pattern in prec_patterns:
            match = re.search(pattern, html_content, re.DOTALL | re.IGNORECASE)
            if match:
                data["precautions"] = self._extract_list_items(match.group(1))
                break

        # 8. Dosage ("al-jur'a" section)
        dose_patterns = [
            r"جرعة[\s\w]*?<p[^>]*>(.*?)</p>",
            r"dosage[\s\w]*?<p[^>]*>(.*?)</p>",
        ]
        for pattern in dose_patterns:
            match = re.search(pattern, html_content, re.DOTALL | re.IGNORECASE)
            if match:
                data["dosage"] = self._clean_html(match.group(1))
                break

        # 9. Storage ("ṭarīqat al-ḥifẓ" section)
        storage_patterns = [
            r"حفظ[\s\w]*?<p[^>]*>(.*?)</p>",
            r"تخزين[\s\w]*?<p[^>]*>(.*?)</p>",
            r"storage[\s\w]*?<p[^>]*>(.*?)</p>",
        ]
        for pattern in storage_patterns:
            match = re.search(pattern, html_content, re.DOTALL | re.IGNORECASE)
            if match:
                data["storage"] = self._clean_html(match.group(1))
                break

        return data

    def _clean_html(self, html_fragment: str) -> str:
        """Clean HTML tags and normalize whitespace."""
        text = re.sub(r"<[^>]+>", " ", html_fragment)
        text = html.unescape(text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def _extract_list_items(self, html_list: str) -> str:
        """Extract items from HTML <ul>/<ol> list."""
        items = re.findall(r"<li[^>]*>(.*?)</li>", html_list, re.DOTALL)
        clean_items = [
            self._clean_html(item) for item in items if self._clean_html(item)
        ]
        return "\n".join(clean_items)


# ============================================================================
# STEP 4: DATABASE MANAGER
# ============================================================================


class DatabaseManager:
    """
    Manages SQLite database operations.
    Creates enriched table, handles inserts, checkpoints.
    """

    def __init__(self, input_db: str, output_db: str):
        self.input_db = input_db
        self.output_db = output_db
        self.conn = None
        self.cursor = None
        self._init_db()

    def _init_db(self):
        """Initialize output database with schema."""
        # Copy input DB to output if doesn't exist
        import shutil

        if not Path(self.output_db).exists():
            shutil.copy(self.input_db, self.output_db)
            print(f"[INFO] Copied {self.input_db} -> {self.output_db}")

        self.conn = sqlite3.connect(self.output_db)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

        # Create descriptions table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS drug_descriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                drug_id INTEGER NOT NULL,
                source TEXT DEFAULT 'altibbi.com',
                source_url TEXT,
                search_query TEXT,
                page_title TEXT,
                meta_description TEXT,
                main_description TEXT,
                uses TEXT,
                side_effects TEXT,
                contraindications TEXT,
                precautions TEXT,
                dosage TEXT,
                storage TEXT,
                status TEXT DEFAULT 'pending',
                error_message TEXT,
                scraped_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (drug_id) REFERENCES drugs(id)
            )
        """)

        # Create indexes
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_desc_drug_id
            ON drug_descriptions(drug_id)
        """)
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_desc_status
            ON drug_descriptions(status)
        """)
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_desc_source
            ON drug_descriptions(source)
        """)

        self.conn.commit()
        print("[INFO] Database initialized successfully")

    def get_drugs_to_process(
        self, limit: Optional[int] = None, status_filter: str = "pending"
    ) -> List[sqlite3.Row]:
        """
        Get drugs that need description enrichment.

        Args:
            limit: Max records to fetch
            status_filter: 'pending', 'not_found', 'error', or 'all'
        """
        query = """
            SELECT d.id, d.trade_name, d.active_ingredient, d.category, d.route
            FROM drugs d
            LEFT JOIN drug_descriptions dd ON d.id = dd.drug_id
            WHERE dd.drug_id IS NULL
        """

        if status_filter != "all":
            query += f" OR dd.status = '{status_filter}'"

        query += " ORDER BY d.id"

        if limit:
            query += f" LIMIT {limit}"

        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_stats(self) -> Dict[str, int]:
        """Get processing statistics."""
        stats = {}

        # Total drugs
        self.cursor.execute("SELECT COUNT(*) FROM drugs")
        stats["total_drugs"] = self.cursor.fetchone()[0]

        # Processed
        self.cursor.execute("SELECT COUNT(*) FROM drug_descriptions")
        stats["total_processed"] = self.cursor.fetchone()[0]

        # By status
        self.cursor.execute("""
            SELECT status, COUNT(*)
            FROM drug_descriptions
            GROUP BY status
        """)
        for row in self.cursor.fetchall():
            stats[f"status_{row[0]}"] = row[1]

        return stats

    def insert_description(
        self,
        drug_id: int,
        data: Dict[str, str],
        search_query: str,
        status: str = "found",
    ):
        """Insert or update drug description."""
        self.cursor.execute(
            """
            INSERT OR REPLACE INTO drug_descriptions (
                drug_id, source, source_url, search_query, page_title,
                meta_description, main_description, uses, side_effects,
                contraindications, precautions, dosage, storage,
                status, scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                drug_id,
                data.get("source", "altibbi.com"),
                data.get("source_url", ""),
                search_query,
                data.get("page_title", ""),
                data.get("meta_description", ""),
                data.get("main_description", ""),
                data.get("uses", ""),
                data.get("side_effects", ""),
                data.get("contraindications", ""),
                data.get("precautions", ""),
                data.get("dosage", ""),
                data.get("storage", ""),
                status,
                data.get("scraped_at", datetime.now().isoformat()),
            ),
        )

    def mark_not_found(self, drug_id: int, search_query: str):
        """Mark drug as not found on Altibbi."""
        self.cursor.execute(
            """
            INSERT OR REPLACE INTO drug_descriptions
            (drug_id, search_query, status, scraped_at)
            VALUES (?, ?, 'not_found', ?)
        """,
            (drug_id, search_query, datetime.now().isoformat()),
        )

    def mark_error(self, drug_id: int, search_query: str, error: str):
        """Mark drug with error."""
        self.cursor.execute(
            """
            INSERT OR REPLACE INTO drug_descriptions
            (drug_id, search_query, status, error_message, scraped_at)
            VALUES (?, ?, 'error', ?, ?)
        """,
            (drug_id, search_query, error, datetime.now().isoformat()),
        )

    def checkpoint(self):
        """Commit current transaction."""
        self.conn.commit()

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.commit()
            self.conn.close()


# ============================================================================
# STEP 5: MAIN PIPELINE ORCHESTRATOR
# ============================================================================


class EnrichmentPipeline:
    """
    Main orchestrator that runs the complete 7-step enrichment process.
    """

    def __init__(
        self,
        input_db: str = Config.INPUT_DB,
        output_db: str = Config.OUTPUT_DB,
    ):
        self.db = DatabaseManager(input_db, output_db)
        self.searcher = DuckDuckGoSearcher()
        self.scraper = AltibbiScraper()
        self.normalizer = NameNormalizer()
        self.stats = {
            "processed": 0,
            "found": 0,
            "not_found": 0,
            "errors": 0,
            "skipped": 0,
        }

    def run(
        self,
        limit: Optional[int] = None,
        category_filter: Optional[str] = None,
    ):
        """
        Run the enrichment pipeline.

        Args:
            limit: Max drugs to process (None = all)
            category_filter: Only process specific category (e.g., 'ANTIBIOTIC')
        """
        print("=" * 80)
        print("DRUGGED APP - ALTIBBI DESCRIPTION ENRICHMENT PIPELINE")
        print("=" * 80)
        print(f"Input DB:  {Config.INPUT_DB}")
        print(f"Output DB: {Config.OUTPUT_DB}")
        print("Search:    DuckDuckGo HTML (Free, Unlimited)")
        print("Target:    Altibbi.com")
        print("=" * 80)

        # Get drugs to process
        drugs = self.db.get_drugs_to_process(limit=limit)

        if category_filter:
            drugs = [d for d in drugs if d["category"] == category_filter]

        total = len(drugs)
        print(f"[INFO] Found {total} drugs to process")

        if total == 0:
            print("[INFO] No drugs to process. Exiting.")
            return

        # Process each drug
        for i, drug in enumerate(drugs, 1):
            print(
                f"\n[{i}/{total}] Processing: {drug['trade_name']} (ID: {drug['id']})"
            )
            self._process_drug(drug)

            # Checkpoint
            if i % Config.CHECKPOINT_INTERVAL == 0:
                self.db.checkpoint()
                print(f"[CHECKPOINT] Saved progress ({i}/{total})")
                self._print_stats()

        # Final save
        self.db.checkpoint()
        print("\n" + "=" * 80)
        print("PIPELINE COMPLETE")
        print("=" * 80)
        self._print_stats(final=True)

    def _process_drug(self, drug: sqlite3.Row):
        """Process a single drug through the pipeline."""
        drug_id = drug["id"]
        trade_name = drug["trade_name"]
        active_ingredient = drug["active_ingredient"]

        # Step 1: Normalize name
        clean_name = self.normalizer.normalize(trade_name)
        if not clean_name:
            print("  [SKIP] Empty name after normalization")
            self.stats["skipped"] += 1
            return

        print(f"  Clean name: {clean_name}")

        # Step 2: Generate search queries
        queries = self.normalizer.generate_search_queries(
            trade_name, active_ingredient
        )

        # Step 3: Search and find Altibbi URL
        altibbi_url = None
        search_query_used = None

        for query in queries:
            print(f"  Searching: {query[:60]}...")
            results = self.searcher.search(query)

            if results:
                # Find first Altibbi result
                for result in results:
                    if "altibbi.com" in result["url"]:
                        altibbi_url = result["url"]
                        search_query_used = query
                        print(f"  [FOUND] URL: {altibbi_url[:70]}...")
                        break

                if altibbi_url:
                    break

            time.sleep(random.uniform(1, 2))  # Between query variations

        if not altibbi_url:
            print("  [NOT FOUND] No Altibbi page found")
            self.db.mark_not_found(drug_id, queries[0] if queries else "")
            self.stats["not_found"] += 1
            return

        # Step 4: Scrape Altibbi page
        print(f"  Scraping: {altibbi_url[:60]}...")
        page_data = self.scraper.scrape(altibbi_url)

        if not page_data:
            print("  [ERROR] Failed to scrape page")
            self.db.mark_error(drug_id, search_query_used, "Scrape failed")
            self.stats["errors"] += 1
            return

        # Step 5: Validate content quality
        quality_score = self._assess_quality(page_data)
        print(f"  Quality score: {quality_score}/10")

        if quality_score < 3:
            print("  [LOW QUALITY] Insufficient content")
            self.db.mark_error(
                drug_id, search_query_used, "Low quality content"
            )
            self.stats["errors"] += 1
            return

        # Step 6: Save to database
        self.db.insert_description(
            drug_id, page_data, search_query_used, "found"
        )
        self.stats["found"] += 1

        # Show preview
        preview = page_data.get("meta_description", "") or page_data.get(
            "main_description", ""
        )
        if preview:
            print(f"  Preview: {preview[:100]}...")

        self.stats["processed"] += 1

    def _assess_quality(self, data: Dict[str, str]) -> int:
        """
        Assess quality of scraped content (0-10 scale).
        """
        score = 0

        # Has title (1 point)
        if data.get("page_title") and len(data["page_title"]) > 5:
            score += 1

        # Has meta description (2 points)
        if data.get("meta_description") and len(data["meta_description"]) > 20:
            score += 2

        # Has main description (3 points)
        if data.get("main_description") and len(data["main_description"]) > 50:
            score += 3

        # Has uses (2 points)
        if data.get("uses") and len(data["uses"]) > 10:
            score += 2

        # Has side effects (2 points)
        if data.get("side_effects") and len(data["side_effects"]) > 10:
            score += 2

        return score

    def _print_stats(self, final: bool = False):
        """Print current statistics."""
        label = "FINAL STATS" if final else "CURRENT STATS"
        print(f"\n[{label}]")
        print(f"  Processed:   {self.stats['processed']}")
        print(f"  Found:       {self.stats['found']}")
        print(f"  Not Found:   {self.stats['not_found']}")
        print(f"  Errors:      {self.stats['errors']}")
        print(f"  Skipped:     {self.stats['skipped']}")
        print(f"  Total:       {sum(self.stats.values())}")

        db_stats = self.db.get_stats()
        print("\n[DATABASE]")
        print(f"  Total drugs: {db_stats.get('total_drugs', 0)}")
        print(f"  Processed:   {db_stats.get('total_processed', 0)}")
        for key, val in db_stats.items():
            if key.startswith("status_"):
                print(f"  {key}: {val}")

    def close(self):
        """Cleanup resources."""
        self.db.close()


# ============================================================================
# STEP 6: UTILITY FUNCTIONS
# ============================================================================


def export_to_csv(db_path: str, output_csv: str = "drug_descriptions.csv"):
    """
    Export enriched descriptions to CSV for easy viewing.
    """
    try:
        import pandas as pd
    except ImportError:
        print(
            "[WARNING] pandas not installed. Install with: pip install pandas"
        )
        return

    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(
        """
        SELECT
            d.id,
            d.trade_name,
            d.active_ingredient,
            d.category,
            dd.source_url,
            dd.page_title,
            dd.meta_description,
            dd.main_description,
            dd.uses,
            dd.side_effects,
            dd.status,
            dd.scraped_at
        FROM drugs d
        LEFT JOIN drug_descriptions dd ON d.id = dd.drug_id
        ORDER BY d.id
    """,
        conn,
    )

    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"[EXPORT] Saved to {output_csv}")
    conn.close()


def merge_into_original(input_db: str, enriched_db: str):
    """
    Merge enriched descriptions back into original database.
    Adds new columns to existing drugs table.
    """
    conn = sqlite3.connect(input_db)
    cursor = conn.cursor()

    # Add description column to existing drugs table
    try:
        cursor.execute("""
            ALTER TABLE drugs ADD COLUMN description TEXT
        """)
        cursor.execute("""
            ALTER TABLE drugs ADD COLUMN altibbi_url TEXT
        """)
        cursor.execute("""
            ALTER TABLE drugs ADD COLUMN description_status TEXT DEFAULT 'pending'
        """)
    except sqlite3.OperationalError:
        print("[INFO] Columns already exist")

    # Copy data from enriched DB
    conn_enriched = sqlite3.connect(enriched_db)
    cursor_enriched = conn_enriched.cursor()
    cursor_enriched.execute("""
        SELECT drug_id, source_url, meta_description, main_description, status
        FROM drug_descriptions
        WHERE status = 'found'
    """)

    for row in cursor_enriched.fetchall():
        drug_id, url, meta, main, status = row
        description = meta or main or ""

        cursor.execute(
            """
            UPDATE drugs
            SET description = ?, altibbi_url = ?, description_status = ?
            WHERE id = ?
        """,
            (description, url, status, drug_id),
        )

    conn.commit()
    print("[MERGE] Descriptions merged into original database")
    conn.close()
    conn_enriched.close()


# ============================================================================
# STEP 7: MAIN ENTRY POINT
# ============================================================================


def main():
    """
    Main entry point. Handles command-line arguments and runs pipeline.
    """
    parser = argparse.ArgumentParser(
        description="Enrich Drugged App database with Altibbi descriptions"
    )
    parser.add_argument(
        "--input",
        "-i",
        default=Config.INPUT_DB,
        help="Input SQLite database path (default: drugged.db)",
    )
    parser.add_argument(
        "--output",
        "-o",
        default=Config.OUTPUT_DB,
        help="Output SQLite database path (default: drugged_enriched.db)",
    )
    parser.add_argument(
        "--limit",
        "-l",
        type=int,
        default=None,
        help="Limit number of drugs to process",
    )
    parser.add_argument(
        "--category",
        "-c",
        default=None,
        help="Filter by category (e.g., ANTIBIOTIC)",
    )
    parser.add_argument(
        "--export-csv",
        action="store_true",
        help="Export results to CSV after processing",
    )
    parser.add_argument(
        "--merge",
        action="store_true",
        help="Merge descriptions back into original DB",
    )
    parser.add_argument(
        "--test", action="store_true", help="Test mode: process only 5 drugs"
    )

    args = parser.parse_args()

    # Update config
    Config.INPUT_DB = args.input
    Config.OUTPUT_DB = args.output

    # Test mode
    if args.test:
        print("[TEST MODE] Processing 5 drugs only")
        args.limit = 5

    # Run pipeline
    pipeline = EnrichmentPipeline(args.input, args.output)

    try:
        pipeline.run(limit=args.limit, category_filter=args.category)
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Saving checkpoint...")
        pipeline.db.checkpoint()
    finally:
        pipeline.close()

    # Export to CSV if requested
    if args.export_csv:
        export_to_csv(args.output)

    # Merge into original if requested
    if args.merge:
        merge_into_original(args.input, args.output)

    print("\n[DONE]")


if __name__ == "__main__":
    main()
