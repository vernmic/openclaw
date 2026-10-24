import { extractSqliteTableSchema } from "../infra/sqlite-schema-sql.js";

export const RECLAMATION_SCHEMA_VERSION = 25;
export function reclamationSchemaSql(schema: string): string {
  return extractSqliteTableSchema(schema, "session_reclamation_jobs", {
    endMarker: "-- End resumable reclamation schema.",
    includeEndMarker: false,
  });
}
export function withoutReclamationSchema(schema: string): string {
  return schema.includes("CREATE TABLE IF NOT EXISTS session_reclamation_jobs (")
    ? schema.replace(reclamationSchemaSql(schema), "")
    : schema;
}
