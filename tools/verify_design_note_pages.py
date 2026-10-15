import subprocess
import os
import re
import html

def markdown_to_html(md_text):
    # Math and symbol replacements
    replacements = [
        (r'\$\\le\s*1\.0\\,\text\{s\}\$', '≤ 1.0 s'),
        (r'\$\\le\s*1\.0\s*s\$', '≤ 1.0 s'),
        (r'\$\\Delta\s*i\s*=\s*1\$', 'Δi = 1'),
        (r'\$\[i_\{?\\text\{prev\}?\}\s*\+\s*1,\s*i_\{?\\text\{curr\}?\}\s*-\s*1\]\$', '[i_prev + 1, i_curr - 1]'),
        (r'\$\\sqrt\{\\text\{ch1\}\^2\s*\+\s*\\text\{ch2\}\^2\}\$', '√(ch1² + ch2²)'),
        (r'\$400w\$', '400w'),
        (r'\$400w\s*\+\s*399\$', '400w + 399'),
        (r'\$400\(w\s*-\s*4\)\$', '400(w - 4)'),
        (r'\$400\(w\s*\+\s*1\)\s*-\s*1\$', '400(w + 1) - 1'),
        (r'\$i\s*\\ge\s*0\$', 'i ≥ 0'),
        (r'\$<\s*3\.5\\,\text\{s\}\$', '< 3.5 s'),
        (r'\$<\s*3\.5\s*s\$', '< 3.5 s'),
        (r'\$[-8,388,608, 8,388,607]\$', '[-8,388,608, 8,388,607]'),
        (r'\[-8,388,608,\s*8,388,607\]', '[-8,388,608, 8,388,607]'),
        (r'\\le', '≤'),
        (r'\\ge', '≥'),
    ]
    for pattern, repl in replacements:
        md_text = re.sub(pattern, repl, md_text)

    # Clean any stray dollar signs
    md_text = md_text.replace('$', '')

    lines = md_text.split('\n')
    html_body = []
    in_table = False
    table_html = []
    in_mermaid = False

    def format_inline(txt):
        # Convert `code` to <code>code</code>
        txt = re.sub(r'`([^`]+)`', r'<code>\1</code>', txt)
        # Convert **bold** to <strong>bold</strong>
        txt = re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', txt)
        # Convert *italic* to <em>italic</em>
        txt = re.sub(r'\*([^*]+)\*', r'<em>\1</em>', txt)
        return txt

    svg_diagram = """
    <div class="diagram-container">
        <svg viewBox="0 0 760 110" class="arch-svg">
            <defs>
                <marker id="arrow" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                    <path d="M 0 1 L 8 5 L 0 9 z" fill="#2563eb" />
                </marker>
            </defs>
            <!-- Edge Node Box -->
            <rect x="5" y="5" width="460" height="100" rx="6" fill="#f8fafc" stroke="#94a3b8" stroke-width="1.5" stroke-dasharray="4 2"/>
            <text x="15" y="22" font-size="11" font-weight="bold" fill="#334155">EDGE NODE (Raspberry Pi 4 / ARM64)</text>
            
            <!-- Edge Components -->
            <rect x="20" y="35" width="105" height="55" rx="4" fill="#eff6ff" stroke="#3b82f6" stroke-width="1.5"/>
            <text x="72" y="55" font-size="9.5" font-weight="bold" fill="#1e3a8a" text-anchor="middle">TCP Mock</text>
            <text x="72" y="72" font-size="8.5" fill="#1e40af" text-anchor="middle">Port 7100 (400 Hz)</text>

            <line x1="125" y1="62" x2="155" y2="62" stroke="#2563eb" stroke-width="1.5" marker-end="url(#arrow)"/>
            
            <rect x="155" y="35" width="135" height="55" rx="4" fill="#eff6ff" stroke="#3b82f6" stroke-width="1.5"/>
            <text x="222" y="55" font-size="9.5" font-weight="bold" fill="#1e3a8a" text-anchor="middle">Protocol & CRC32</text>
            <text x="222" y="72" font-size="8.5" fill="#1e40af" text-anchor="middle">Atomic Ingestion</text>

            <line x1="290" y1="62" x2="320" y2="62" stroke="#2563eb" stroke-width="1.5" marker-end="url(#arrow)"/>

            <rect x="320" y="35" width="130" height="55" rx="4" fill="#eff6ff" stroke="#3b82f6" stroke-width="1.5"/>
            <text x="385" y="52" font-size="9.5" font-weight="bold" fill="#1e3a8a" text-anchor="middle">SQLite WAL Log</text>
            <text x="385" y="66" font-size="8" fill="#1e40af" text-anchor="middle">sync=FULL (≤1s loss)</text>
            <text x="385" y="79" font-size="8" fill="#1e40af" text-anchor="middle">Window & Dual Sync</text>

            <!-- Edge to Cloud Connection -->
            <line x1="465" y1="62" x2="520" y2="62" stroke="#2563eb" stroke-width="1.8" marker-end="url(#arrow)"/>
            <text x="492" y="55" font-size="8" font-weight="bold" fill="#2563eb" text-anchor="middle">HTTPS</text>
            <text x="492" y="75" font-size="7.5" fill="#64748b" text-anchor="middle">2s Gzip Chunks</text>

            <!-- Cloud Box -->
            <rect x="520" y="5" width="230" height="100" rx="6" fill="#fefce8" stroke="#ca8a04" stroke-width="1.5" stroke-dasharray="4 2"/>
            <text x="530" y="22" font-size="11" font-weight="bold" fill="#854d0e">CLOUD PLATFORM (Cloudflare)</text>
            
            <rect x="535" y="35" width="95" height="55" rx="4" fill="#fef9c3" stroke="#eab308" stroke-width="1.5"/>
            <text x="582" y="55" font-size="9.5" font-weight="bold" fill="#713f12" text-anchor="middle">Worker Engine</text>
            <text x="582" y="72" font-size="8.5" fill="#854d0e" text-anchor="middle">Online Windowing</text>

            <line x1="630" y1="62" x2="655" y2="62" stroke="#ca8a04" stroke-width="1.5" marker-end="url(#arrow)"/>

            <rect x="655" y="35" width="85" height="55" rx="4" fill="#fef9c3" stroke="#eab308" stroke-width="1.5"/>
            <text x="697" y="55" font-size="9.5" font-weight="bold" fill="#713f12" text-anchor="middle">D1 SQLite</text>
            <text x="697" y="72" font-size="8.5" fill="#854d0e" text-anchor="middle">Idempotent Sync</text>
        </svg>
    </div>
    """

    for line in lines:
        s = line.strip()
        if s.startswith('```mermaid') or s.startswith('```'):
            if in_mermaid:
                in_mermaid = False
                html_body.append(svg_diagram)
            else:
                in_mermaid = True
            continue

        if in_mermaid:
            continue

        if s.startswith('# '):
            html_body.append(f'<h1>{format_inline(s[2:])}</h1>')
        elif s.startswith('### '):
            html_body.append(f'<h3>{format_inline(s[4:])}</h3>')
        elif s.startswith('* '):
            html_body.append(f'<li>{format_inline(s[2:])}</li>')
        elif s.startswith('|') and s.endswith('|'):
            if not in_table:
                in_table = True
                table_html = ['<table class="styled-table">']
            cells = [c.strip() for c in s[1:-1].split('|')]
            if not all(c.startswith('---') or c.startswith(':---') or c.endswith('---') for c in cells):
                is_header = len(table_html) == 1
                tag = 'th' if is_header else 'td'
                row = ''.join(f'<{tag}>{format_inline(c)}</{tag}>' for c in cells)
                table_html.append(f'<tr>{row}</tr>')
        else:
            if in_table:
                table_html.append('</table>')
                html_body.extend(table_html)
                table_html = []
                in_table = False

            if s == '---':
                html_body.append('<hr/>')
            elif s:
                html_body.append(f'<p>{format_inline(s)}</p>')

    if in_table:
        table_html.append('</table>')
        html_body.extend(table_html)

    return '\n'.join(html_body)

def verify_pages():
    html_path = os.path.abspath('tools/design_note_render.html')
    pdf_path = os.path.abspath('docs/DESIGN_NOTE.pdf')
    md_path = os.path.abspath('docs/DESIGN_NOTE.md')

    with open(md_path, 'r', encoding='utf-8') as f:
        md_content = f.read()

    rendered_body = markdown_to_html(md_content)

    full_html = f'''<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>MayaCardio System Design Note</title>
<style>
@page {{
    size: letter;
    margin: 0.45in 0.5in 0.45in 0.5in;
}}
body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    font-size: 8.5pt;
    line-height: 1.25;
    color: #1e293b;
    margin: 0;
    padding: 0;
}}
h1 {{
    font-size: 12pt;
    font-weight: 800;
    color: #0f172a;
    margin: 0 0 2px 0;
    letter-spacing: -0.2px;
}}
h3 {{
    font-size: 9.2pt;
    font-weight: 700;
    color: #1e3a8a;
    margin: 4px 0 2px 0;
    border-bottom: 1px solid #cbd5e1;
    padding-bottom: 1px;
}}
p {{
    margin: 1.5px 0;
}}
li {{
    margin: 1px 0;
    margin-left: 14px;
}}
hr {{
    border: 0;
    border-top: 1px solid #e2e8f0;
    margin: 3px 0;
}}
code {{
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    font-size: 7.8pt;
    background: #f1f5f9;
    padding: 1px 3px;
    border-radius: 3px;
    color: #0f172a;
    border: 1px solid #e2e8f0;
}}
strong {{
    color: #0f172a;
}}
.diagram-container {{
    margin: 3px 0;
    text-align: center;
}}
.arch-svg {{
    width: 100%;
    max-height: 95px;
}}
.styled-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 7.5pt;
    margin: 3px 0;
    page-break-inside: avoid;
}}
.styled-table th {{
    background: #f8fafc;
    color: #0f172a;
    font-weight: 700;
    border: 1px solid #cbd5e1;
    padding: 3px 5px;
    text-align: left;
}}
.styled-table td {{
    border: 1px solid #cbd5e1;
    padding: 2.5px 5px;
    text-align: left;
    vertical-align: top;
}}
</style>
</head>
<body>
{rendered_body}
</body>
</html>'''

    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(full_html)

    edge_exe = r'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe'
    cmd = [
        edge_exe,
        '--headless',
        '--disable-gpu',
        '--no-pdf-header-footer',
        f'--print-to-pdf={pdf_path}',
        html_path
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print(f"Edge print failed: {res.stderr}")
        return

    # Count pages in PDF using standard PDF object inspection
    with open(pdf_path, 'rb') as f:
        pdf_bytes = f.read()

    page_matches = re.findall(rb'/Type\s*/Page\b', pdf_bytes)
    page_count = len(page_matches)
    print(f"PAGINATED RENDER SUCCESSFUL:")
    print(f"Tool: Microsoft Edge (Chromium) Headless PDF Engine")
    print(f"Arguments: --headless --disable-gpu --no-pdf-header-footer")
    print(f"Page Dimensions: Letter (8.5 x 11 in)")
    print(f"Rendered Output Path: {pdf_path}")
    print(f"Verified Exact Page Count: {page_count} pages")

if __name__ == '__main__':
    verify_pages()
