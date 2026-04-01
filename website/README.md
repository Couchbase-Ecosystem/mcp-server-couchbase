# Couchbase MCP Server Documentation

This website is built using [Docusaurus](https://docusaurus.io/), a modern static website generator.

## Prerequisites

- **Node.js 20+** — Required to run Docusaurus. Check with `node --version`.
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
├── docs/                       # Documentation source files (.md / .mdx)
│   ├── 01-overview.md          # Homepage (slug: /)
│   ├── 02-get-started/         # Prerequisites, quickstart, registries
│   ├── 04-tools/               # Tool reference documentation
│   ├── 05-configuration/       # Environment variables, read-only mode, etc.
│   ├── 06-security.md          # Security considerations and best practices
│   └── 07-build-from-source.md # Guide to building from source and running locally
│   ├── 08-product-notes/       # Release notes and contributing guide
├── src/
│   └── css/
│       └── custom.css          # Custom styles and theme overrides
├── static/
│   └── img/                    # Images (logos, architecture diagrams, etc.)
├── docusaurus.config.js        # Site configuration (navbar, footer, plugins)
├── docusaurus.config.js        # Site configuration (navbar, footer, plugins)
├── sidebars.js                 # Sidebar structure configuration
├── package.json                # npm dependencies and scripts
├── package-lock.json           # npm lock file
└── .markdownlint.jsonc         # Markdownlint configuration
```

## Adding or Editing Pages

- **Docs** live in `docs/`. Files are named with a numeric prefix (`01-`, `02-`) which controls sidebar ordering automatically - no `sidebar_position` frontmatter needed.
- **Frontmatter** is only required when you need to override defaults:
  - `slug` - custom URL (e.g. `slug: /` for the homepage)
  - `sidebar_label` - sidebar label when it differs from the H1
- **MDX files** that use JSX imports (e.g. `<Tabs>`) need frontmatter or a comment before the imports to satisfy markdownlint MD041.
- **Internal links** should use relative file paths (e.g. `../configuration/01-environment-variables.md`) so Docusaurus validates them at build time.

## Linting

Markdownlint is configured in `.markdownlint.jsonc`. Key rules in effect:

| Rule | Status | Reason |
| ---- | ------ | ------ |
| MD013 (line length) | Disabled | Allows long URLs and command examples |
| MD033 (inline HTML) | Disabled | Required for JSX components and HTML elements |
| MD041 (first line heading) | Disabled | MDX files start with imports, not headings |

To check for lint errors, use the markdownlint VS Code extension or run:

```bash
npx markdownlint-cli 'docs/**/*.md'
```

## Deployment

The site is automatically deployed to GitHub Pages via GitHub Actions when changes to `website/` are pushed to `main`. See `.github/workflows/deploy-docs.yml`.

The live site is available at: <https://mcp-server.couchbase.com/>
