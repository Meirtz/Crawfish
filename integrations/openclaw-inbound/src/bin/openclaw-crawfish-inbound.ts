#!/usr/bin/env node

import { CrawfishUdsClient } from "../client.js";
import { OpenClawInboundBridge, runStdioBridge } from "../server.js";

const client = new CrawfishUdsClient();
const bridge = new OpenClawInboundBridge(client);

runStdioBridge(bridge).catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
