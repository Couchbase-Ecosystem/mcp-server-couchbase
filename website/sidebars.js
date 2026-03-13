// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docsSidebar: [
    'overview',
    {
      type: 'category',
      label: 'Get Started',
      items: [
        'get-started/prerequisites',
        'get-started/quickstart',
      ],
    },
    'troubleshooting',
    {
      type: 'category',
      label: 'Advanced Setup',
      items: ['installation/source', 'installation/docker'],
    },
    {
      type: 'category',
      label: 'Configuration',
      link: {
        type: 'doc',
        id: 'configuration/index',
      },
      items: [
        'configuration/environment-variables',
        'configuration/authentication',
        'configuration/read-only-mode',
        'configuration/disabling-tools',
        'configuration/transport-modes',
      ],
    },
    'tools/cluster-health',
    'registries',
    'contributing',
    'security',
    'release-notes',
  ],
};

export default sidebars;
