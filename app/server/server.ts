import { createApp, analytics, server, createWorkspaceClient } from "@databricks/appkit";
import { z } from "zod";

const RetornoSchema = z.object({
  cliente_id: z.coerce.number(),
  vendedor: z.string(),
  status: z.enum(["vendeu", "vai_pensar", "sem_interesse", "nao_atendeu"]),
  comentario: z.string().max(500).optional().default(""),
  referencia: z.string().nullable().transform((v) => v ?? ""),
});

await createApp({
  plugins: [analytics(), server()],
  async onPluginsReady(appkit) {
    const wsClient = createWorkspaceClient({});

    appkit.server.extend((app) => {
      app.get("/api/quem-sou", (_req, res) => {
        const auth = _req.get("X-Forwarded-User") || "usuario@rotaperfume.com.br";
        res.json({ nome: auth, email: auth });
      });

      app.get("/api/test", (_req, res) => {
        res.json({ ok: true, ts: Date.now() });
      });

      app.post("/api/retorno", async (req, res) => {
        console.log("[api/retorno] hit, body:", JSON.stringify(req.body));

        const parsed = RetornoSchema.safeParse(req.body);
        if (!parsed.success) {
          console.log("[api/retorno] validation error:", JSON.stringify(parsed.error.issues));
          res.status(400).json({
            erro: "Parâmetros inválidos",
            detalhe: parsed.error.issues.map((i) => `${i.path.join(".")}: ${i.message}`).join("; "),
          });
          return;
        }

        const { cliente_id, vendedor, status, comentario, referencia } = parsed.data;
        console.log("[api/retorno] parsed ok:", { cliente_id, vendedor, status, referencia });

        try {
          const warehouseId = process.env.DATABRICKS_WAREHOUSE_ID || "";
          console.log("[api/retorno] warehouseId:", warehouseId);

          const sql = `
            MERGE INTO lakehouse_rotaperfume.gold.retorno_ligacao t
            USING (SELECT ${cliente_id} AS cliente_id) s
            ON t.cliente_id = s.cliente_id
            WHEN MATCHED THEN UPDATE SET
              t.status = '${status}',
              t.comentario = '${comentario.replace(/'/g, "''")}',
              t.registrado_em = current_timestamp(),
              t.registrado_por = '${vendedor.replace(/'/g, "''")}',
              t._referencia = current_date()
            WHEN NOT MATCHED THEN INSERT
              (cliente_id, vendedor, _referencia, status, comentario, registrado_em, registrado_por)
            VALUES
              (${cliente_id}, '${vendedor.replace(/'/g, "''")}', current_date(), '${status}', '${comentario.replace(/'/g, "''")}', current_timestamp(), '${vendedor.replace(/'/g, "''")}')
          `;

          console.log("[api/retorno] calling statementExecution...");
          const response = await wsClient.statementExecution.executeStatement({
            warehouse_id: warehouseId,
            statement: sql,
            catalog: "lakehouse_rotaperfume",
            schema: "gold",
          });

          console.log("[api/retorno] SQL done, statement_id:", response.statement_id);
          if (response.statement_id) {
            res.json({ ok: true });
          } else {
            res.status(500).json({ erro: "Falha ao executar SQL" });
          }
        } catch (e: unknown) {
          console.error("[api/retorno] CATCH:", e);
          res.status(500).json({ erro: "Erro ao gravar retorno no banco" });
        }
      });
    });
  },
}).catch(console.error);
