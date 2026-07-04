#!/usr/bin/env python3
import sys
import os
import unicodedata

BIDI_DANGEROUS = {
    '\u202A': 'LEFT-TO-RIGHT EMBEDDING (LRE)',
    '\u202B': 'RIGHT-TO-LEFT EMBEDDING (RLE)',
    '\u202C': 'POP DIRECTIONAL FORMATTING (PDF)',
    '\u202D': 'LEFT-TO-RIGHT OVERRIDE (LRO)',
    '\u202E': 'RIGHT-TO-LEFT OVERRIDE (RLO)',
    '\u2066': 'LEFT-TO-RIGHT ISOLATE (LRI)',
    '\u2067': 'RIGHT-TO-LEFT ISOLATE (RLI)',
    '\u2068': 'FIRST STRONG ISOLATE (FSI)',
    '\u2069': 'POP DIRECTIONAL ISOLATE (PDI)',
}

TARGET_EXTENSIONS = ('.py', '.sh', '.yml', '.yaml', '.md', 'Makefile')
EXCLUDE_DIRS = ('.git', 'artifacts', 'dist', '__pycache__')

def scan_file(file_path):
    violations = []
    try:
        with open(file_path, 'r', encoding='utf-8', errors='surrogateescape') as f:
            for line_num, line in enumerate(f, start=1):
                for char_idx, char in enumerate(line):
                    if char in BIDI_DANGEROUS:
                        violations.append((line_num, char_idx, f"BIDI [{BIDI_DANGEROUS[char]}]"))
                    elif unicodedata.category(char) == 'Cf' and char not in ['\t', '\n', '\r']:
                        violations.append((line_num, char_idx, f"Invisible Control Character (Cf: U+{ord(char):04X})"))
    except Exception as e:
        print(f"Skipping file {file_path} due to read error: {e}")
    return violations

def main():
    print("========== Starting qtop Codebase Trust & Sanity Audit ==========")
    total_violations = 0

    for root, dirs, files in os.walk('.'):
        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]

        for file in files:
            if file.endswith(TARGET_EXTENSIONS) or file == 'Makefile':
                file_path = os.path.join(root, file)
                clean_path = os.path.normpath(file_path)

                violations = scan_file(clean_path)
                if violations:
                    total_violations += len(violations)
                    print(f"\n[CRITICAL SANITY FAILURE] -> {clean_path}")
                    for line, col, desc in violations:
                        print(f"  Line {line}, Col {col}: Found {desc}")

    print("\n=================== Audit Summary ===================")
    if total_violations > 0:
        print(f"Result: FAILED. Found {total_violations} untrusted character sequences.")
        sys.exit(1)
    else:
        print("Result: PASSED. No malicious BIDI or hidden Unicode characters detected.")
        sys.exit(0)

if __name__ == '__main__':
    main()
