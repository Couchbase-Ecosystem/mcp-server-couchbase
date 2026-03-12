// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docsSidebar: [
    {
      type: 'category',
      label: 'Home',
      items: ['home/introduction', 'home/tutorials'],
    },
    {
      type: 'category',
      label: 'Get Started',
      items: [
        'get-started/prerequisites',
        'get-started/quickstart',
        'get-started/setup',
        'get-started/test-and-usage',
        'get-started/troubleshooting',
      ],
    },
    {
      type: 'category',
      label: 'Installation Methods',
      items: ['installation/uv', 'installation/source', 'installation/docker'],
    },
    {
      type: 'category',
      label: 'Configuration',
      items: [
        'configuration/environment-variables',
        'configuration/authentication',
        'configuration/read-only-mode',
        'configuration/disabling-tools',
        'configuration/transport-modes',
        'configuration/troubleshooting',
      ],
    },
    {
      type: 'category',
      label: 'Tools',
      items: [
        'tools/cluster-health',
        'tools/data-model',
        'tools/kv-operations',
        'tools/query-indexing',
        'tools/performance-analysis',
      ],
    },
    {
      type: 'category',
      label: 'Environments / IDEs',
      items: [
        'environments/cursor',
        'environments/claude-desktop',
        'environments/windsurf',
        'environments/vscode',
        'environments/jetbrains',
      ],
    },
    'registries',
    'contributing',
    'support-policy',
    'security',
    'release-notes',
  ],
};

export default sidebars;
