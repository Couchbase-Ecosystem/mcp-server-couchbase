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
        'get-started/registries',
      ],
    },
    'troubleshooting',
    'tools/cluster-health',
    {
      type: 'category',
      label: 'Configuration',
      link: {
        type: 'doc',
        id: 'configuration/index',
      },
      items: [
        'configuration/environment-variables',
        'configuration/read-only-mode',
        'configuration/disabling-tools',
      ],
    },
    {
      type: 'category',
      label: 'Run from Source or Docker',
      items: ['installation/source', 'installation/docker'],
    },
    'security',
    {
      type: 'category',
      label: 'Product Notes',
      items: [
        'product-notes/release-notes',
        'product-notes/contributing',
      ],
    },
  ],
};

export default sidebars;
