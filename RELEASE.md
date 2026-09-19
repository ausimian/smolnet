### Changed

- The three ExDoc guides moved from `guides/` to the repository root, so
  `gen_tcp.md`, `gen_udp.md`, and `socket_api.md` now sit beside `README.md`.
  Links that addressed a guide under `guides/` no longer resolve.

### Fixed

- Guide links in the published documentation no longer point at a path that
  does not exist. ExDoc rewrites relative `.md` links in extras only for its
  HTML formatter; the Markdown formatter writes each page's source verbatim,
  and every rendered page offers that mirror through a "Copy Markdown" link.
  A reader who reached the README that way was handed `guides/gen_tcp.md`, a
  path that exists in the repository but not in the docs tarball, where
  extras are flattened to the root. ExDoc cannot place an extra in a
  subdirectory of its output, so the guides now live where the docs put them
  and one relative link resolves on GitHub, in the rendered documentation,
  and in the Markdown mirror alike.
