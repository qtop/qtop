#!/usr/bin/env python3
"""Convert RST documentation.md (actually still RST format) to proper Markdown."""
import re
import sys

def rst_to_md(rst_text):
    lines = rst_text.split('\n')
    result = []
    i = 0
    
    # State tracking
    in_table = False
    in_code_block = False
    
    while i < len(lines):
        line = lines[i]
        
        # Handle code blocks (RST :: syntax)
        if line.strip() == '::':
            result.append('```')
            in_code_block = True
            i += 1
            continue
        
        if in_code_block:
            if line.strip() == '' and i+1 < len(lines) and lines[i+1].strip() == '':
                result.append('')
                result.append('')
                result.append('```')
                result.append('')
                in_code_block = False
                i += 1
                continue
            result.append(line)
            i += 1
            continue
        
        # Skip RST comment lines
        if line.strip().startswith('.. '):
            i += 1
            continue
        
        # Check for RST section headers
        # H1: ==== underline
        # H2: ---- underline  
        # H3: ~~~~ underline
        if i + 1 < len(lines):
            next_line = lines[i + 1]
            if re.match(r'^=+\s*$', next_line):
                result.append(f'# {line.strip()}')
                i += 2
                continue
            elif re.match(r'^-+\s*$', next_line) and not re.match(r'^-\s', line) and not re.match(r'^\s*$', line):
                result.append(f'## {line.strip()}')
                i += 2
                continue
            elif re.match(r'^~+\s*$', next_line):
                result.append(f'### {line.strip()}')
                i += 2
                continue
        
        # Convert RST links: `text <url>`__ -> [text](url)
        # Handle both single-line and multi-line patterns
        line = re.sub(r'`([^`<]+?)\s*<([^>]+)>`__?\s*', r'[\1](\2)', line)
        
        # Convert RST inline literals: ``text`` -> `text`
        line = re.sub(r'``(.+?)``', r'`\1`', line, flags=re.DOTALL)
        
        # Convert RST emphasis: *text* (but not markdown emphasis)
        # RST uses *text* for italics, same as markdown
        
        # Convert RST definition lists (terminology)
        # Lines starting with a non-indented term followed by indented definition
        
        # Handle RST list items: -  text or #. text
        line = re.sub(r'^-\s{2}', '- ', line)
        line = re.sub(r'^#\.\s', '1. ', line)
        
        # Handle RST field lists: :field: value -> **field**: value
        line = re.sub(r'^:([^:]+):\s*', r'**\1**: ', line)
        
        # Handle separator
        if re.match(r'^[-]{3,}\s*$', line):
            result.append('---')
            i += 1
            continue
        
        # Handle table-like RST (simple pipe table)
        if re.match(r'^[+][-=+]', line):
            i += 1
            continue
        
        # Skip continuation lines in RST tables
        if re.match(r'^[|]', line):
            i += 1
            continue
        
        result.append(line)
        i += 1
    
    # Clean up empty lines
    text = '\n'.join(result)
    # Remove multiple consecutive blank lines
    text = re.sub(r'\n{4,}', '\n\n\n', text)
    return text

if __name__ == '__main__':
    with open(sys.argv[1], 'r') as f:
        content = f.read()
    
    md = rst_to_md(content)
    
    with open(sys.argv[1], 'w') as f:
        f.write(md)
    
    print(f"Converted {sys.argv[1]}")

