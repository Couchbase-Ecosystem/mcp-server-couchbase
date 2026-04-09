// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docsSidebar: [
    "overview",
    {
      type: "category",
      label: "Get Started",
      items: [
        "get-started/prerequisites",
        "get-started/quickstart",
        "get-started/registries",
      ],
    },
    "troubleshooting",
    "tools",
    {
      type: "category",
      label: "Configuration",
      link: {
        type: "doc",
        id: "configuration/index",
      },
      items: [
        "configuration/environment-variables",
        "configuration/read-only-mode",
        "configuration/streamable-http",
        "configuration/disabling-tools",
        "configuration/elicitation-for-tools",
      ],
    },
    "security",
    "build-from-source",
    {
      type: "category",
      label: "Product Notes",
      items: ["product-notes/release-notes"],
    },
    {
      type: "category",
      label: "Contributing",
      items: ["contributing/server", "contributing/docs"],
    },
  ],
};

export default sidebars;
