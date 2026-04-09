# Documentation

Guide for contributing to the Couchbase MCP Server documentation site.

If you want to contribute to the server itself, see [Contributing to the MCP Server](./01-server.md).

The documentation is built with [Docusaurus](https://docusaurus.io/) and lives in the `website/` directory of the repository.

## Prerequisites

- **Node.js 20+** - Check with `node --version`.
- **npm** - Comes with Node.js.

## Local Development

```bash
cd website
npm install
npm start
```

Opens a local dev server at `http://localhost:3000`. Most changes are reflected live without restarting.

## Project Structure

```bash
website/
├── docs/                       # Documentation source files (.md / .mdx)
│   ├── 01-overview.md          # Homepage (slug: /)
│   ├── 02-get-started/         # Prerequisites, quickstart, registries
│   ├── 04-tools.md             # Tool reference documentation
│   ├── 05-configuration/       # Environment variables, read-only mode, etc.
│   ├── 06-security.md          # Security considerations and best practices
│   ├── 07-build-from-source.md # Guide to building from source and running locally
│   ├── 08-product-notes/       # Release notes and contributing guides
│   └── 09-contributing/        # Contributing guides

├── src/
│   └── css/
│       └── custom.css          # Custom styles and theme overrides
├── static/
│   └── img/                    # Images (logos, diagrams, GIFs, etc.)
├── docusaurus.config.js        # Site configuration (navbar, footer, plugins)
├── sidebars.js                 # Sidebar structure configuration
├── package.json                # npm dependencies and scripts
├── package-lock.json           # npm lock file
└── .markdownlint.jsonc         # Markdownlint configuration
```

## Adding or Editing Pages

- **Docs** live in `docs/`. Files are named with a numeric prefix (`01-`, `02-`) which controls sidebar ordering — no `sidebar_position` frontmatter needed.
- **Frontmatter** is only required when overriding defaults:
  - `slug` — custom URL (e.g. `slug: /` for the homepage)
  - `sidebar_label` — sidebar label when it differs from the H1
- **MDX files** that use JSX (e.g. `<Tabs>`) need frontmatter or a comment before the imports to satisfy markdownlint MD041.
- **Internal links** should use relative file paths (e.g. `../05-configuration/01-environment-variables.md`) so Docusaurus validates them at build time.

## Linting

Markdownlint is configured in `.markdownlint.jsonc`. Key rules:

| Rule | Status | Reason |
| ---- | ------ | ------ |
| MD013 (line length) | Disabled | Allows long URLs and command examples |
| MD033 (inline HTML) | Disabled | Required for JSX components and HTML elements |
| MD041 (first line heading) | Disabled | MDX files start with imports, not headings |

To check for lint errors:

```bash
npx markdownlint-cli 'docs/**/*.md'
```

## Building for Production

```bash
npm run build
```

Generates a static build in `build/`. Always run this before opening a PR - it catches broken links and MDX compilation errors that `npm start` won't surface.

To preview the production build locally:

```bash
npm run serve
```

## Deployment

The site is automatically deployed to [GitHub Pages](https://couchbase-ecosystem.github.io/mcp-server-couchbase/) via GitHub Actions when changes to `website/` are pushed to `main`. See [`.github/workflows/deploy-docs.yml`](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/blob/main/.github/workflows/deploy-docs.yml).

PR previews are also deployed automatically - a preview URL is posted as a comment on each PR that touches `website/`.
