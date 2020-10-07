.PHONY: all pdf html clean

default: html


all: pdf html
pdf: out/radicle-link.pdf
html: out/radicle-link.html
tex: out/radicle-link.tex

sections := $(wildcard sections/*.md)

out:
	mkdir -p out

# FIXME: `--citeproc` produces LaTeX my texlive doesn't understand, so need to
# continue using `--filter=pandoc-citeproc` for PDF for now.
out/radicle-link.pdf: out pandoc/template.latex pandoc/ieee-with-url.csl references.bib
	pandoc \
		index.md \
		$(sections) \
		--standalone \
		--toc \
		--from=markdown \
		--template=pandoc/template.latex \
		--filter=pandoc-citeproc \
		--csl=pandoc/ieee-with-url.csl \
		-o $@

out/radicle-link.html: index.md $(sections) out out/radicle-link.pdf out/spec.css pandoc/template.html pandoc/ieee-with-url.csl references.bib
	pandoc \
		index.md \
		$(sections) \
		--standalone \
		--toc \
		--from=markdown \
		--template=pandoc/template.html \
		--metadata=pdfn:radicle-link.pdf \
		--css=spec.css \
		--mathjax \
		--citeproc \
		--csl=pandoc/ieee-with-url.csl \
		-o $@

out/spec.css: out pandoc/spec.css
	cp pandoc/spec.css out/spec.css

clean:
	rm -rf out
