# Couchbase MCP Server

An [MCP](https://modelcontextprotocol.io/) server implementation of Couchbase that allows LLMs to directly interact with Couchbase clusters.

## Features

- Get a list of all the scopes and collections in the specified bucket in a Couchbase cluster
- Get the structure for a collection in a Couchbase cluster
- Run a [SQL++ query](https://www.couchbase.com/sqlplusplus/) against data in Couchbase cluster bucket

## Prerequisites

- Python 3.10 or higher.
- A running Couchbase cluster. The easiest way to get started is to use [Capella](https://docs.couchbase.com/cloud/get-started/create-account.html#getting-started) free tier, which is fully managed version of Couchbase server. You can follow [instructions](https://docs.couchbase.com/cloud/clusters/data-service/import-data-documents.html#import-sample-data) to import one of the sample datasets or import your own.
- [uv](https://docs.astral.sh/uv/) installed to run the server.
- An [MCP client](https://modelcontextprotocol.io/clients) such as [Claude Desktop](https://claude.ai/download) installed to connect the server to Claude. The instructions are provided for Claude Desktop and Cursor. Other MCP clients could be used as well.

## Configuration

Clone the repository to your local machine.

```bash
git clone https://github.com/Couchbase-Ecosystem/mcp-server-couchbase.git
```

### Claude Desktop

Follow the steps below to use Couchbase MCP server with Claude Desktop MCP client

1. The MCP server can now be added to Claude Desktop by editing the configuration file. More detailed instructions can be found on the [MCP quickstart guide](https://modelcontextprotocol.io/quickstart/user).

   - On Mac, the configuration file is located at `~/Library/Application Support/Claude/claude_desktop_config.json`
   - On Windows, the configuration file is located at `%APPDATA%\Claude\claude_desktop_config.json`

   Open the configuration file and add the following configuration to the `mcpServers` section:

   ```json
   {
     "mcpServers": {
       "couchbase": {
         "command": "uv",
         "args": [
           "--directory",
           "path/to/cloned/repo/mcp-server-couchbase/",
           "run",
           "src/mcp_server.py"
         ],
         "env": {
           "CB_CONNECTION_STRING": "couchbases://connection-string",
           "CB_USERNAME": "username",
           "CB_PASSWORD": "password",
           "CB_BUCKET_NAME": "bucket_name"
         }
       }
     }
   }
   ```

   The server can be configured using environment variables. The following variables are supported:

   - `CB_CONNECTION_STRING`: The connection string to the Couchbase cluster
   - `CB_USERNAME`: The username with access to the bucket to use to connect
   - `CB_PASSWORD`: The password for the username to connect
   - `CB_BUCKET_NAME`: The name of the bucket that the server will access
   - `path/to/cloned/repo/mcp-server-couchbase/` should be the path to the cloned repository on your local machine. Don't forget the trailing slash at the end!

2. Restart Claude Desktop to apply the changes.

3. You can now use the server in Claude Desktop to run queries on the Couchbase cluster using natural language.

#### Claude Desktop Logs

The logs for Claude Desktop can be found in the following locations:

- MacOS: ~/Library/Logs/Claude
- Windows: %APPDATA%\Claude\Logs

The logs can be used to diagnose connection issues or other problems with your MCP server configuration. For more details, refer to the [official documentation](https://modelcontextprotocol.io/quickstart/user#getting-logs-from-claude-for-desktop).

### Cursor

Follow steps below to use Couchbase MCP server with Cursor:

1. Install [Cursor](https://cursor.sh/) on your machine.

2. In Cursor, go to Cursor > Cursor Settings > MCP > Add a new global MCP server. Also, checkout the docs on [setting up MCP server configuration](https://docs.cursor.com/context/model-context-protocol#configuring-mcp-servers) from Cursor.

3. Specify the same configuration as above. You may need to add the server configuration under a parent key of mcpServers like this below.

```json
{
  "mcpServers": {
    "couchbase": {
      "command": "uv",
      "args": [
        "--directory",
        "path/to/cloned/repo/mcp-server-couchbase/",
        "run",
        "src/mcp_server.py"
      ],
      "env": {
        "CB_CONNECTION_STRING": "couchbases://connection-string",
        "CB_USERNAME": "username",
        "CB_PASSWORD": "password",
        "CB_BUCKET_NAME": "bucket_name"
      }
    }
  }
}
```

The server can be configured using environment variables. The following variables are supported:

- `CB_CONNECTION_STRING`: The connection string to the Couchbase cluster
- `CB_USERNAME`: The username with access to the bucket to use to connect
- `CB_PASSWORD`: The password for the username to connect
- `CB_BUCKET_NAME`: The name of the bucket that the server will access
- `path/to/cloned/repo/mcp-server-couchbase/` should be the path to the cloned repository on your local machine. Don't forget the trailing slash at the end!

4. Save the configuration.

5. You will see couchbase as an added server in MCP list. Refresh to see if server is enabled.

6. You can now use the Couchbase MCP server in Cursor to query your Couchbase cluster using natural language.

For more details about MCP integration with Cursor, refer to the [official Cursor MCP documentation](https://docs.cursor.sh/ai-features/mcp-model-context-protocol).

#### Cursor Logs

In the bottom panel of Cursor, click on "Output" and select "Cursor MCP" from the dropdown menu to view server logs. This can help diagnose connection issues or other problems with your MCP server configuration.

## Troubleshooting Tips

- Ensure the path to your MCP server repository is correct in the configuration.
- Verify that your Couchbase connection string, database username, password and bucket name are correct.
- If using Couchbase Capella, ensure that the cluster is [accessible](https://docs.couchbase.com/cloud/clusters/allow-ip-address.html) from the machine where the MCP server is running.
- Check that the database user has proper permissions to access the specified bucket.
- Confirm that the uv package manager is properly installed and accessible. You may need to provide absolute path to uv in the `command` field in the configuration.
- Check the logs for any errors or warnings that may indicate issues with the MCP server. The server logs are under the name, `mcp-server-couchbase.log`.

---

## 📢 Support Policy

We truly appreciate your interest in this project!  
This project is **community-maintained**, which means it's **not officially supported** by our support team.

If you need help, have found a bug, or want to contribute improvements, the best place to do that is right here — by [opening a GitHub issue](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/issues).  
Our support portal is unable to assist with requests related to this project, so we kindly ask that all inquiries stay within GitHub.

Your collaboration helps us all move forward together — thank you!
