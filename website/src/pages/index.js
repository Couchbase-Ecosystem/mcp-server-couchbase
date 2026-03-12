import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';
import styles from './index.module.css';

const FeatureList = [
  {
    title: '23 Tools, 5 Categories',
    description:
      'Cluster health monitoring, schema discovery, KV operations, SQL++ queries, index management, and query performance analysis.',
  },
  {
    title: 'Secure by Default',
    description:
      'Read-only mode enabled by default. Supports RBAC, mTLS, tool disabling, and TLS encryption for defense in depth.',
  },
  {
    title: 'Multi-Client Support',
    description:
      'Works with Claude Desktop, Cursor, VS Code, Windsurf, JetBrains IDEs, and any MCP-compatible client.',
  },
  {
    title: 'Flexible Deployment',
    description:
      'Install from PyPI, run from source, or use Docker. Supports stdio, HTTP, and SSE transport modes.',
  },
];

function Feature({title, description}) {
  return (
    <div className={clsx('col col--3')}>
      <div className="text--center padding-horiz--md padding-vert--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <Heading as="h1" className="hero__title">
          {siteConfig.title}
        </Heading>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link
            className="button button--secondary button--lg"
            to="/docs/get-started/quickstart">
            Get Started
          </Link>
          <Link
            className="button button--outline button--secondary button--lg"
            style={{marginLeft: '1rem'}}
            to="/docs/tools/cluster-health">
            Tool Reference
          </Link>
        </div>
      </div>
    </header>
  );
}

export default function Home() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title="Documentation"
      description="Connect LLMs to Couchbase clusters via the Model Context Protocol">
      <HomepageHeader />
      <main>
        <section className={styles.features}>
          <div className="container">
            <div className="row">
              {FeatureList.map((props, idx) => (
                <Feature key={idx} {...props} />
              ))}
            </div>
          </div>
        </section>
      </main>
    </Layout>
  );
}
