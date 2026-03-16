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
        'configuration/authentication',
        'configuration/read-only-mode',
        'configuration/disabling-tools',
      ],
    },
    {
      type: 'category',
      label: 'Advanced Setup',
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
    'troubleshooting',
  ],
};

export default sidebars;
