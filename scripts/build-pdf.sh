#!/usr/bin/env bash
#
# Renders docs/pdf/*.md to PDF.
#
# Needs pandoc and a LaTeX engine:
#   brew install pandoc basictex          # macOS
#   apt install pandoc texlive-xetex      # Debian/Ubuntu

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

require pandoc

# Optional: a local pdf-export helper. Set PDF_SKILL_DIR to use one; otherwise plain pandoc is used, which
# produces the same output because the options below match what the helper passes.
SKILL="${PDF_SKILL_DIR:-}/scripts/md_to_pdf.py"

for md in "${REPO_ROOT}"/docs/pdf/*.md; do
  [[ -e "$md" ]] || { warn "no markdown in docs/pdf"; exit 0; }
  pdf="${md%.md}.pdf"
  step "$(basename "$md") -> $(basename "$pdf")"
  if [[ -n "${PDF_SKILL_DIR:-}" && -f "$SKILL" ]]; then
    python3 "$SKILL" --input "$md" --output "$pdf" --style report
  else
    # Same options the skill uses, so the output matches without it.
    pandoc "$md" -o "$pdf" --pdf-engine=xelatex --toc --toc-depth=2 \
      -V geometry:margin=1in -V fontsize=11pt -V documentclass=article
    ok "wrote $pdf"
  fi
done
