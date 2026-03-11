// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docsSidebar: [
    {
      type: 'category',
      label: 'Getting Started',
      items: [
        'getting-started/introduction',
        'getting-started/prerequisites',
        'getting-started/installation',
        'getting-started/quickstart',
      ],
    },
    {
      type: 'category',
      label: 'Configuration',
      items: [
        'configuration/environment-variables',
        'configuration/authentication',
        'configuration/read-only-mode',
        'configuration/disabling-tools',
      ],
    },
    {
      type: 'category',
      label: 'MCP Client Guides',
      items: [
        'client-guides/claude-desktop',
        'client-guides/cursor',
        'client-guides/vscode',
        'client-guides/windsurf',
        'client-guides/jetbrains',
      ],
    },
    'transport-modes',
    {
      type: 'category',
      label: 'Tool Reference',
      items: [
        'tools/cluster-health',
        'tools/data-model',
        'tools/kv-operations',
        'tools/query-indexing',
        'tools/performance-analysis',
      ],
    },
    'docker',
    'security',
    {
      type: 'category',
      label: 'Contributing',
      items: [
        'contributing/development-setup',
        'contributing/code-quality',
        'contributing/adding-tools',
        'contributing/testing',
      ],
    },
    'troubleshooting',
  ],
};

export default sidebars;
