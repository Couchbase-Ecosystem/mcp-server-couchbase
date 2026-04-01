import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import ThemedImage from '@theme/ThemedImage';

# Quick Start

Get the Couchbase MCP Server running in under 5 minutes.

## Prerequisites

Make sure you complete the [prerequisites](./01-prerequisites.md) before proceeding.

## Step 1: Configure Your MCP Client

Select your MCP client and add the Couchbase MCP Server configuration.

<Tabs groupId="mcp-client">

<TabItem value="claude-desktop" label="Claude Desktop">

1. Open the Claude Desktop configuration file:

   - **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`

   - **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

2. Add the Couchbase MCP Server configuration. For detailed instructions, see the [MCP quickstart guide](https://modelcontextprotocol.io/quickstart/user).

<Tabs groupId="install-method">
<TabItem value="uvx" label="uvx" default>

```json
{
  "mcpServers": {
    "couchbase": {
      "command": "uvx",
      "args": ["couchbase-mcp-server"],
      "env": {
        "CB_CONNECTION_STRING": "couchbases://your-connection-string",
        "CB_USERNAME": "your-username",
        "CB_PASSWORD": "your-password"
      }
    }
  }
}
```

</TabItem>

<TabItem value="docker" label="Docker">

```json
{
  "mcpServers": {
    "couchbase": {
      "command": "docker",
      "args": [
        "run", "--rm", "-i",
        "-e", "CB_CONNECTION_STRING=couchbases://your-connection-string",
        "-e", "CB_USERNAME=your-username",
        "-e", "CB_PASSWORD=your-password",
        "couchbaseecosystem/mcp-server-couchbase"
      ]
    }
  }
}
```

</TabItem>
</Tabs>

**Logs:** `~/Library/Logs/Claude` (macOS), `%APPDATA%\Claude\Logs` (Windows)

</TabItem>

<TabItem value="cursor" label="Cursor" default>

**Option 1: Add via the Cursor MCP Links**

<a href="https://cursor.com/en-US/install-mcp?name=Couchbase&config=eyJjb21tYW5kIjoidXZ4IiwiYXJncyI6WyJjb3VjaGJhc2UtbWNwLXNlcnZlciJdLCJlbnYiOnsiQ0JfQ09OTkVDVElPTl9TVFJJTkciOiJjb3VjaGJhc2VzOi8vY29ubmVjdGlvbi1zdHJpbmciLCJDQl9VU0VSTkFNRSI6InVzZXJuYW1lIiwiQ0JfUEFTU1dPUkQiOiJwYXNzd29yZCJ9fQ%3D%3D">
  <ThemedImage
    alt="Add Couchbase MCP server to Cursor"
    height="32"
    sources={{
      light: 'https://cursor.com/deeplink/mcp-install-light.svg',
      dark: 'https://cursor.com/deeplink/mcp-install-dark.svg',
    }}
  />
</a>

:::note
The install link uses placeholder values from the configuration examples. Update the connection string and credentials after installation.
:::

**Option 2: Manual Addition**

1. Go to **Cursor > Cursor Settings > Tools & Integrations > MCP Tools**. See the [Cursor MCP documentation](https://cursor.com/docs/mcp) for details.

2. Add the Couchbase MCP Server configuration.

You may need to add the server configuration under a parent key of `mcpServers`.

<Tabs groupId="install-method">
<TabItem value="uvx" label="uvx" default>

```json
{
  "mcpServers": {
    "couchbase": {
      "command": "uvx",
      "args": ["couchbase-mcp-server"],
      "env": {
        "CB_CONNECTION_STRING": "couchbases://your-connection-string",
        "CB_USERNAME": "your-username",
        "CB_PASSWORD": "your-password"
      }
    }
  }
}
```

</TabItem>
<TabItem value="docker" label="Docker">

```json
{
  "mcpServers": {
    "couchbase": {
      "command": "docker",
      "args": [
        "run", "--rm", "-i",
        "-e", "CB_CONNECTION_STRING=couchbases://your-connection-string",
        "-e", "CB_USERNAME=your-username",
        "-e", "CB_PASSWORD=your-password",
        "couchbaseecosystem/mcp-server-couchbase"
      ]
    }
  }
}
```

</TabItem>
</Tabs>

**Logs:** Bottom panel > Output dropdown > "Cursor MCP"

</TabItem>

<TabItem value="factory" label="Factory">

See the [Factory AI MCP documentation](https://docs.factory.ai/cli/configuration/mcp) for details.

**Option 1: Droid CLI**

Run the following command in the Droid CLI:

```bash
droid mcp add couchbase-mcp 'uvx couchbase-mcp-server --connection-string=couchbases://your-connection-string --username=your-username --password=your-password' --type stdio
```

**Option 2: Configuration File**

Navigate to the MCP configuration file at `~/.factory/mcp.json` and add the Couchbase MCP Server configuration.

<Tabs groupId="install-method">
<TabItem value="uvx" label="uvx" default>

```json
{
  "mcpServers": {
    "couchbase": {
      "command": "uvx",
      "args": ["couchbase-mcp-server"],
      "env": {
        "CB_CONNECTION_STRING": "couchbases://your-connection-string",
        "CB_USERNAME": "your-username",
        "CB_PASSWORD": "your-password"
      }
    }
  }
}
```

</TabItem>
<TabItem value="docker" label="Docker">

```json
{
  "mcpServers": {
    "couchbase": {
      "command": "docker",
      "args": [
        "run", "--rm", "-i",
        "-e", "CB_CONNECTION_STRING=couchbases://your-connection-string",
        "-e", "CB_USERNAME=your-username",
        "-e", "CB_PASSWORD=your-password",
        "couchbaseecosystem/mcp-server-couchbase"
      ]
    }
  }
}
```

</TabItem>
</Tabs>

**Option 3: Factory MCP Registry**

To configure the Couchbase MCP server using the Factory MCP registry, type `/mcp` in the chat interface to launch the interactive UI, or type `/mcp` in the Droid CLI to access the MCP management menu. Browse the registry for "Couchbase" MCP server, and add it. Once added, the Couchbase MCP configuration template will be appended to `~/.factory/mcp.json`. Update the placeholder values with your specific settings.

</TabItem>

<TabItem value="jetbrains" label="JetBrains">

1. Install the [AI Assistant](https://www.jetbrains.com/help/ai-assistant/mcp.html) or [Junie](https://junie.jetbrains.com/docs/junie-cli-mcp-configuration.html) plugin.

2. Navigate to **Settings > Tools > AI Assistant (or Junie) > MCP Server**.

3. Click **"+"** to add the Couchbase MCP Server configuration and click **Save**.

<Tabs groupId="install-method">
<TabItem value="uvx" label="uvx" default>

```json
{
  "mcpServers": {
    "couchbase": {
      "command": "uvx",
      "args": ["couchbase-mcp-server"],
      "env": {
        "CB_CONNECTION_STRING": "couchbases://your-connection-string",
        "CB_USERNAME": "your-username",
        "CB_PASSWORD": "your-password"
      }
    }
  }
}
```

</TabItem>
<TabItem value="docker" label="Docker">

```json
{
  "mcpServers": {
    "couchbase": {
      "command": "docker",
      "args": [
        "run", "--rm", "-i",
        "-e", "CB_CONNECTION_STRING=couchbases://your-connection-string",
        "-e", "CB_USERNAME=your-username",
        "-e", "CB_PASSWORD=your-password",
        "couchbaseecosystem/mcp-server-couchbase"
      ]
    }
  }
}
```

</TabItem>
</Tabs>

**Logs:** Help > Show Log in Finder/Explorer > mcp > couchbase

</TabItem>

<TabItem value="vscode" label="VS Code">

**Option 1: MCP Server bundled with CouchbaseVS Code Extension**

The [Couchbase VS Code Extension](https://marketplace.visualstudio.com/items?itemName=Couchbase.vscode-couchbase) (v3.0.0+) bundles the MCP server out-of-the-box. It auto-prompts to start MCP server on cluster connect.

From Command Palette (Ctrl+Shift+P / Cmd+Shift+P):

- Couchbase: Start MCP Server - Start the server
- Couchbase: Get MCP Server Config - Inspect config JSON
- Couchbase: MCP Server Settings - Customize disabled tools, elicitation for tools, read-only mode, export paths, etc.

**Quick Start**: Install extension → Connect cluster → Accept prompt to start MCP Server.

**Option 2: Manual Configuration**

See the [VS Code MCP documentation](https://code.visualstudio.com/docs/copilot/chat/mcp-servers) for details.

1. Create a configuration file using one of two scopes:

   - **Workspace** — Create `.vscode/mcp.json` in your project root. Settings apply only to that project.

   - **Global (User)** — Run **MCP: Open User Configuration** from the Command Palette (`Ctrl+Shift+P` / `Cmd+Shift+P`). Settings apply to all projects. On macOS, this file is located at `~/Library/Application Support/Code/User/mcp.json`.

2. Add the Couchbase MCP Server configuration.

:::note
VS Code uses `servers` as the top-level JSON property, not `mcpServers`.
:::

<Tabs groupId="install-method">
<TabItem value="uvx" label="uvx" default>

```json
{
  "servers": {
    "couchbase": {
      "command": "uvx",
      "args": ["couchbase-mcp-server"],
      "env": {
        "CB_CONNECTION_STRING": "couchbases://your-connection-string",
        "CB_USERNAME": "your-username",
        "CB_PASSWORD": "your-password"
      }
    }
  }
}
```

</TabItem>
<TabItem value="docker" label="Docker">

```json
{
  "servers": {
    "couchbase": {
      "command": "docker",
      "args": [
        "run", "--rm", "-i",
        "-e", "CB_CONNECTION_STRING=couchbases://your-connection-string",
        "-e", "CB_USERNAME=your-username",
        "-e", "CB_PASSWORD=your-password",
        "couchbaseecosystem/mcp-server-couchbase"
      ]
    }
  }
}
```

</TabItem>
</Tabs>

**Logs:** Command Palette > "MCP: List Servers" > Show Output

</TabItem>

<TabItem value="windsurf" label="Windsurf">

1. Navigate to **Command Palette > Windsurf MCP Configuration Panel** or **Windsurf - Settings > Advanced > Cascade > Model Context Protocol (MCP) Servers**. See the [official documentation](https://docs.windsurf.com/windsurf/cascade/mcp#adding-a-new-mcp-plugin) for details.

2. Click **Add Server** > **Add custom server**.

3. Add the Couchbase MCP Server configuration.

<Tabs groupId="install-method">
  <TabItem value="uvx" label="uvx" default>

```json
{
  "mcpServers": {
    "couchbase": {
      "command": "uvx",
      "args": ["couchbase-mcp-server"],
      "env": {
        "CB_CONNECTION_STRING": "couchbases://your-connection-string",
        "CB_USERNAME": "your-username",
        "CB_PASSWORD": "your-password"
      }
    }
  }
}
```

</TabItem>
<TabItem value="docker" label="Docker">

```json
{
  "mcpServers": {
    "couchbase": {
      "command": "docker",
      "args": [
        "run", "--rm", "-i",
        "-e", "CB_CONNECTION_STRING=couchbases://your-connection-string",
        "-e", "CB_USERNAME=your-username",
        "-e", "CB_PASSWORD=your-password",
        "couchbaseecosystem/mcp-server-couchbase"
      ]
    }
  }
}
```

</TabItem>
</Tabs>

**Logs:** Check Windsurf output panel

</TabItem>
</Tabs>

:::tip
Replace the placeholder values with your actual Couchbase cluster credentials. See [Environment Variables](/configuration/environment-variables) for all available options.
:::

## Step 2: Set Your Connection String

The connection string format depends on your deployment type.

<Tabs groupId="deployment-type">
<TabItem value="capella" label="Capella" default>

Use `couchbases://` (with `s` for TLS):

```bash
couchbases://cb.your-endpoint.cloud.couchbase.com
```

Find your connection string in the [Capella UI](https://docs.couchbase.com/cloud/get-started/connect.html) under **Cluster > Connect**.

:::important
Ensure the machine running the MCP server has its IP [allowed](https://docs.couchbase.com/cloud/clusters/allow-ip-address.html) in your Capella cluster settings.
:::

</TabItem>
<TabItem value="self-managed" label="Self-Managed">

Use `couchbase://` for local or unencrypted connections:

```bash
couchbase://localhost
couchbase://127.0.0.1
```

Use `couchbases://` if TLS is enabled on your cluster.

To find your connection string and credentials, see the Couchbase Web Console: connection strings are under **Servers** ([Connect to Couchbase Server](https://docs.couchbase.com/server/current/guides/connect.html)), and database users are under **Security** ([Manage Users and Roles](https://docs.couchbase.com/server/current/manage/manage-security/manage-users-and-roles.html)).

If your Couchbase server is running on the host machine and you are using Docker, the connection string would typically be `couchbase://host.docker.internal`. See the [Docker networking documentation](https://docs.docker.com/desktop/features/networking/#i-want-to-connect-from-a-container-to-a-service-on-the-host) for more details.

</TabItem>
</Tabs>

## Step 3: Set Up Authentication

Configure authentication for your MCP server. Choose one of the following methods.

### Basic Authentication (Username and Password)

Set `CB_USERNAME` and `CB_PASSWORD` in your MCP client configuration:

```json
{
  "env": {
    "CB_CONNECTION_STRING": "couchbases://your-connection-string",
    "CB_USERNAME": "your-username",
    "CB_PASSWORD": "your-password"
  }
}
```

### mTLS (Mutual TLS)

Set `CB_CLIENT_CERT_PATH` and `CB_CLIENT_KEY_PATH`:

```json
{
  "env": {
    "CB_CONNECTION_STRING": "couchbases://your-connection-string",
    "CB_CLIENT_CERT_PATH": "/path/to/client-certificate.pem",
    "CB_CLIENT_KEY_PATH": "/path/to/client.key"
  }
}
```

See [Configuring Authentication](../configuration/01-environment-variables.md#configuring-authentication) for full details.

## Step 4: Validate the Setup

Save your configuration and restart your MCP client.

<details>
<summary><b>Check server status</b></summary>

Ask your AI assistant:

> "What is the status of the Couchbase MCP server?"

The assistant will call `get_server_configuration_status` and return the server's configuration, including transport mode, read-only mode status, and loaded tools.

If the tool is not available, the server did not start correctly. Check the logs for your client (listed above in each client tab).

</details>

<details>
<summary><b>Check cluster health and services</b></summary>

> "Check the health of my Couchbase cluster and list all running services."

The assistant will call `get_cluster_health_and_services`, which connects to the cluster and returns the cluster health status along with all running services (Data, Query, Index, Search, etc.).

If the connection fails, verify your `CB_CONNECTION_STRING`, `CB_USERNAME`, and `CB_PASSWORD` values.

</details>

<details>
<summary><b>Try sample queries</b></summary>

Once connected, try these prompts:

- "List all buckets in my cluster"
- "Show me the scopes and collections in the `travel-sample` bucket"
- "What does a document in the airline collection look like?"
- "Find all airlines based in the United States"
- "Run a query: `SELECT COUNT(*) FROM airline`"
- "What are the longest running queries on my cluster?"
- "Check my cluster health"

</details>

## Read Only Mode

By default, the server runs in **read-only mode** (`CB_MCP_READ_ONLY_MODE=true`):

- All KV write tools (upsert, insert, replace, delete) are **not loaded**

- SQL++ queries that modify data are **not loaded**

To enable write operations, set `CB_MCP_READ_ONLY_MODE=false` in your configuration. See [Read-Only Mode](../configuration/02-read-only-mode.md) for details.

## Next Steps

- Review all available [Tools](../04-tools.md)
- Explore [Configuration](../configuration/00-index.md) options
- Want to customize or build from source? See [Build from Source](../07-build-from-source.md)
- Having issues? See [Troubleshooting](../03-troubleshooting.md)
