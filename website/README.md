# Couchbase MCP Server Documentation

This website is built using [Docusaurus 3](https://docusaurus.io/), a modern static website generator.

## Local Development

```bash
cd website
npm install
npm start
```

This starts a local development server at `http://localhost:3000` and opens a browser window. Most changes are reflected live without restarting.

## Build

```bash
npm run build
```

Generates static content in the `build` directory.

## Deployment

The site is automatically deployed to GitHub Pages via GitHub Actions when changes to `website/` are pushed to `main`. See `.github/workflows/deploy-docs.yml`.
