import { copyFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { expect, test } from "vitest";
import { withOpenClawTestState } from "../test-utils/openclaw-test-state.js";
import { withAgentDatabaseMaintenanceLease } from "./openclaw-agent-db-maintenance-lease.js";
import { assertOpenClawAgentCurrentRuntimeSchema } from "./openclaw-agent-db-schema-helpers.js";
import {
  ensureOpenClawAgentDatabaseSchema,
  ensureOpenClawAgentSchema,
} from "./openclaw-agent-db-schema.js";
import { withoutReclamationSchema } from "./openclaw-agent-reclamation-schema.js";
import { OPENCLAW_AGENT_SCHEMA_SQL } from "./openclaw-agent-schema.js";

test("copied v24 migration adds required v25 receipts without rewriting transcripts and refuses rollback", async () => {
  await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
    const sourcePath = state.path("source-v24.sqlite");
    const targetPath = state.path("candidate-v25.sqlite");
    const source = new DatabaseSync(sourcePath);
    try {
      source.exec(withoutReclamationSchema(OPENCLAW_AGENT_SCHEMA_SQL));
      source.exec(`PRAGMA user_version = 24;
        INSERT INTO schema_meta(meta_key,role,schema_version,agent_id,app_version,created_at,updated_at)
          VALUES ('primary','agent',24,'main','2026.10.3',1,1);
        INSERT INTO session_nodes(session_key,current_session_id,entry_json,updated_at) VALUES ('agent:main:kept','kept','{"sessionId":"kept","updatedAt":1}',1);
        INSERT INTO session_windows(session_id,session_key,created_at,updated_at) VALUES ('kept','agent:main:kept',1,1);
        INSERT INTO transcript_rewrite_watermarks(session_id,generation,updated_at) VALUES ('kept','unchanged-generation',1);
        INSERT INTO transcript_events(session_id,seq,event_json,created_at) VALUES ('kept',0,' {"type":"custom","data":{"text":"unchanged"}} ',1);`);
    } finally {
      source.close();
    }
    copyFileSync(sourcePath, targetPath);
    const target = new DatabaseSync(targetPath);
    try {
      const before = target.prepare("SELECT rowid,* FROM transcript_events").all();
      await withAgentDatabaseMaintenanceLease({ env: state.env }, async () => {
        ensureOpenClawAgentDatabaseSchema(target, {
          agentId: "main",
          path: targetPath,
          env: state.env,
        });
      });
      expect(target.prepare("PRAGMA user_version").get()).toEqual({ user_version: 25 });
      expect(target.prepare("SELECT schema_version FROM schema_meta").get()).toEqual({
        schema_version: 25,
      });
      expect(target.prepare("SELECT rowid,* FROM transcript_events").all()).toEqual(before);
      expect(target.prepare("SELECT generation FROM transcript_rewrite_watermarks").get()).toEqual({
        generation: "unchanged-generation",
      });
      expect(
        target.prepare("SELECT count(*) AS count FROM session_reclamation_jobs").get(),
      ).toEqual({ count: 0 });
      expect(() => ensureOpenClawAgentSchema(target, "main", targetPath, 24)).toThrow(
        /expected at most 24/,
      );
      target.exec("DROP TABLE session_reclamation_jobs");
      expect(() =>
        assertOpenClawAgentCurrentRuntimeSchema(target, { agentId: "main", pathname: targetPath }),
      ).toThrow();
      expect(
        target
          .prepare("SELECT name FROM sqlite_schema WHERE name = 'session_reclamation_jobs'")
          .get(),
      ).toBeUndefined();
    } finally {
      target.close();
    }
  });
});
