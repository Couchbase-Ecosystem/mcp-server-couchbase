# Couchbase MCP Server Documentation

This website is built using [Docusaurus](https://docusaurus.io/), a modern static website generator.

## Prerequisites

- **Node.js 20+** — pinned in [.nvmrc](.nvmrc). With `nvm` installed, run `nvm use` in this directory. Check with `node --version`.
- **npm** — Comes with Node.js.

## Local Development

```bash
cd website
npm install
npm start
```

This starts a local development server at `http://localhost:3000` and opens a browser window. Most changes are reflected live without restarting the server.

## Build

```bash
npm run build
```

Generates a static production build in the `build/` directory. Run this to catch broken links and other build-time errors before deploying.

To preview the production build locally:

```bash
npm run serve
```

## Project Structure

```bash
website/
├── versioned_docs/             # Per-version documentation snapshots
│   ├── version-0.8/            # Current released docs (served at /)
│   └── version-0.7/            # Older released docs (served at /0.7/)
├── versioned_sidebars/         # Sidebar snapshots, one per version
├── versions.json               # Ordered list of versions (newest first)
├── i18n/en/code.json           # Translation overrides (e.g. version banner text)
├── src/
│   └── css/
│       └── custom.css          # Custom styles and theme overrides
├── static/
│   └── img/                    # Images (logos, architecture diagrams, etc.)
├── docusaurus.config.js        # Site configuration (navbar, footer, plugins)
├── sidebars.js                 # Sidebar template (used when cutting new versions)
├── package.json                # npm dependencies and scripts
├── package-lock.json           # npm lock file
├── .nvmrc                      # Pinned Node version
└── .markdownlint.jsonc         # Markdownlint configuration
```

## Editing the right version

This site is versioned, and `includeCurrentVersion: false` is set in `docusaurus.config.js`. **All edits go into `versioned_docs/`** — there is no unversioned `docs/` folder. Pick the version that matches your intent:

- **`versioned_docs/version-0.8/`** — the **current released docs**, served at `/`. Edit here for typo fixes, clarifications, or any change to the live site.
- **`versioned_docs/version-0.7/`** — older released docs, served at `/0.7/`. Edit here only for fixes that apply specifically to the 0.7 release.

**Rule of thumb:** if your PR's intent is to change what users see *today*, edit `versioned_docs/version-0.8/`. If a fix applies to both 0.7 and 0.8, edit both files in the same PR.

### Anatomy of a version snapshot

Each version is a self-contained directory under `versioned_docs/`, paired with a sidebar file under `versioned_sidebars/`. The folder structure inside maps directly to URL paths — files with a numeric prefix (`01-`, `02-`) control sidebar ordering.

```bash
versioned_docs/version-0.8/                         # → /<page>          (lastVersion)
├── 01-overview.md                                  # → /                (homepage, slug: /)
├── 02-get-started/
│   ├── 01-prerequisites.md                         # → /get-started/prerequisites
│   ├── 02-quickstart.md                            # → /get-started/quickstart
│   └── 03-registries.md                            # → /get-started/registries
├── 03-troubleshooting.md                           # → /troubleshooting
├── 04-tools.md                                     # → /tools
├── 05-configuration/
│   ├── 00-index.md                                 # → /configuration
│   ├── 01-environment-variables.md                 # → /configuration/environment-variables
│   ├── 02-read-only-mode.md                        # → /configuration/read-only-mode
│   └── ...
├── 06-security.md                                  # → /security
├── 07-build-from-source.md                         # → /build-from-source
├── 08-product-notes/
│   └── 01-release-notes.md                         # → /product-notes/release-notes
└── 09-contributing/
    ├── 01-server.md                                # → /contributing/server
    └── 02-docs.md                                  # → /contributing/docs

versioned_sidebars/
├── version-0.8-sidebars.json                       # Snapshot of sidebars.js at the time 0.8 was cut
└── version-0.7-sidebars.json
```

For a non-`lastVersion` like 0.7, URLs are prefixed with the version: `/0.7/tools`, `/0.7/configuration/read-only-mode`, etc.

Note that each version's sidebar is frozen at snapshot time — restructuring `sidebars.js` later only affects future versions. The same applies to file structure: renaming a file in 0.8 doesn't touch 0.7.

### Cutting a new version

New versions are created by **copying the latest snapshot and editing on a feature branch**, not via `docusaurus docs:version`. Review happens in the PR (the PR preview deploys each version under its own URL).

To cut e.g. 1.0 from the current 0.8:

```bash
cd website

# 1. Copy the latest snapshot
cp -R versioned_docs/version-0.8 versioned_docs/version-1.0
cp versioned_sidebars/version-0.8-sidebars.json versioned_sidebars/version-1.0-sidebars.json

# 2. Prepend "1.0" to versions.json (newest first):
#    ["1.0", "0.8", "0.7"]

# 3. Edit versioned_docs/version-1.0/ to reflect 1.0 changes

# 4. Bump lastVersion in docusaurus.config.js: "1.0"

# 5. Build to verify
npm run build
```

Use **major.minor** for version labels (`0.8`, `1.0`) — patch releases edit the existing snapshot in place rather than getting their own folder.

## Adding or Editing Pages

- **Docs** live under `versioned_docs/version-<X>/`. Files are named with a numeric prefix (`01-`, `02-`) which controls sidebar ordering automatically - no `sidebar_position` frontmatter needed.
- **Frontmatter** is only required when you need to override defaults:
  - `slug` - custom URL (e.g. `slug: /` for the homepage)
  - `sidebar_label` - sidebar label when it differs from the H1
- **MDX files** that use JSX imports (e.g. `<Tabs>`) need frontmatter or a comment before the imports to satisfy markdownlint MD041.
- **Internal links** should use relative file paths (e.g. `../05-configuration/01-environment-variables.md`) so Docusaurus validates them at build time.

## Linting

Markdownlint is configured in `.markdownlint.jsonc`. Key rules in effect:

| Rule | Status | Reason |
| ---- | ------ | ------ |
| MD013 (line length) | Disabled | Allows long URLs and command examples |
| MD033 (inline HTML) | Disabled | Required for JSX components and HTML elements |
| MD041 (first line heading) | Disabled | MDX files start with imports, not headings |

To check for lint errors, use the markdownlint VS Code extension or run:

```bash
npx markdownlint-cli 'versioned_docs/**/*.md'
```

## Deployment

The site is automatically deployed to GitHub Pages via GitHub Actions when changes to `website/` are pushed to `main`. See `.github/workflows/deploy-docs.yml`.

The live site is available at: <https://mcp-server.couchbase.com/>
