# Configuration parser contract

qtop uses a bounded, dependency-free YAML subset, not a general YAML loader.
The shipped qtopconf.yaml and scheduler fixtures are executable examples.

- Documents contain mapping blocks separated by blank lines.
- A line contains a key ending in a colon, optionally followed by a value.
- Indented keys open nested mappings. Dash entries form lists of text or
  mappings; subsequent mapping fields continue the last mapping item.
- Bracketed lists and quoted Python literals use the bounded literal parser.
- Full-line comments, inline space-hash comments and document start markers
  are recognized. Comment splitting is lexical, including inside quotes.
- A pipe block consumes verbatim lines until indentation decreases.
- Indentation uses two spaces for configuration and four for OAR fixtures;
  historical half-width transitions remain supported for scheduler samples.
- Anchors, aliases, tags and general YAML escaping have no defined semantics.
  Do not use them in configuration. Unsupported indentation transitions and
  incompatible list/mapping continuations raise ValueError.

Input bytes, line length and nesting depth are limited by qtop_py.safety.
Container transition checks live in _list_container and _extend_list; changes
must retain the configuration and OAR sample tests and deterministic fuzz gate.
