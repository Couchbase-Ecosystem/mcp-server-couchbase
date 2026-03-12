// @ts-check

import {themes as prismThemes} from 'prism-react-renderer';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Couchbase MCP Server',
  tagline: 'Connect LLMs to Couchbase clusters via the Model Context Protocol',
  favicon: 'img/favicon.ico',

  future: {
    v4: true,
  },

  url: 'https://Couchbase-Ecosystem.github.io',
  baseUrl: process.env.BASE_URL || '/mcp-server-couchbase/',

  organizationName: 'Couchbase-Ecosystem',
  projectName: 'mcp-server-couchbase',
  trailingSlash: false,

  onBrokenLinks: 'throw',

  markdown: {
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js',
          editUrl:
            'https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/tree/main/website/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      colorMode: {
        respectPrefersColorScheme: true,
      },
      navbar: {
        title: 'Couchbase MCP Server',
        items: [
          {
            type: 'dropdown',
            label: 'Docs',
            position: 'left',
            items: [
              {
                label: 'Home',
                to: '/docs/home/introduction',
              },
              {
                label: 'Get Started',
                to: '/docs/get-started/quickstart',
              },
              {
                label: 'Installation',
                to: '/docs/installation/uv',
              },
              {
                label: 'Configuration',
                to: '/docs/configuration/environment-variables',
              },
              {
                label: 'Tools',
                to: '/docs/tools/cluster-health',
              },
              {
                label: 'Environments / IDEs',
                to: '/docs/environments/cursor',
              },
            ],
          },
          {
            href: 'https://pypi.org/project/couchbase-mcp-server/',
            label: 'PyPI',
            position: 'right',
          },
          {
            href: 'https://hub.docker.com/r/couchbaseecosystem/mcp-server-couchbase',
            label: 'Docker Hub',
            position: 'right',
          },
          {
            href: 'https://github.com/Couchbase-Ecosystem/mcp-server-couchbase',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Documentation',
            items: [
              {
                label: 'Getting Started',
                to: '/docs/home/introduction',
              },
              {
                label: 'Tool Reference',
                to: '/docs/tools/cluster-health',
              },
              {
                label: 'Configuration',
                to: '/docs/configuration/environment-variables',
              },
            ],
          },
          {
            title: 'Resources',
            items: [
              {
                label: 'PyPI Package',
                href: 'https://pypi.org/project/couchbase-mcp-server/',
              },
              {
                label: 'Docker Hub',
                href: 'https://hub.docker.com/r/couchbaseecosystem/mcp-server-couchbase',
              },
              {
                label: 'MCP Protocol',
                href: 'https://modelcontextprotocol.io/',
              },
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'GitHub',
                href: 'https://github.com/Couchbase-Ecosystem/mcp-server-couchbase',
              },
              {
                label: 'Issues',
                href: 'https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/issues',
              },
              {
                label: 'Contributing',
                to: '/docs/contributing',
              },
            ],
          },
        ],
        copyright: `Copyright ${new Date().getFullYear()} Couchbase, Inc. Licensed under Apache 2.0. Built with Docusaurus.`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
        additionalLanguages: ['bash', 'json', 'python'],
      },
    }),
};

export default config;
