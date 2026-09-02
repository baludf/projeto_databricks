import { z } from "zod";

// =============================================================================
// Schema de validacao com Zod
// =============================================================================

const RetornoSchema = z.object({
  cliente_id: z.coerce.number().int().positive(),
  vendedor: z.string().min(1, "vendedor e obrigatorio"),
  status: z.enum(["vendeu", "vai_pensar", "sem_interesse", "nao_atendeu"], {
    errorMap: () => ({
      message: "status deve ser: vendeu, vai_pensar, sem_interesse ou nao_atendeu",
    }),
  }),
  comentario: z.string().max(500, "comentario maximo 500 caracteres").optional().default(""),
  referencia: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, "referencia deve ser aaaa-mm-dd"),
});

type RetornoInput = z.infer<typeof RetornoSchema>;

// =============================================================================
// Plugin de rotas do servidor
// =============================================================================

export function onPluginsReady({ app, getExecutionContext }: any) {
  // ---------------------------------------------------------------------------
  // GET /api/quem-sou — retorna o e-mail do usuario logado
  // ---------------------------------------------------------------------------
  app.get("/api/quem-sou", (req: any, res: any) => {
    const email = req.headers["x-forwarded-email"] || "dev@exemplo.com";
    res.json({ email });
  });

  // ---------------------------------------------------------------------------
  // POST /api/retorno — grava o retorno da ligacao em gold.retorno_ligacao
  // ---------------------------------------------------------------------------
  app.post("/api/retorno", async (req: any, res: any) => {
    // 1. Validar corpo com Zod ANTES de tocar no banco
    const parsed = RetornoSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({
        erro: "Corpo invalido",
        detalhe: parsed.error.issues.map((i) => `${i.path.join(".")}: ${i.message}`).join("; "),
        valores_aceitos: ["vendeu", "vai_pensar", "sem_interesse", "nao_atendeu"],
      });
    }

    const input: RetornoInput = parsed.data;
    const ctx = getExecutionContext();
    const email = req.headers["x-forwarded-email"] || "dev@exemplo.com";

    try {
      // 2. INSERT com parameters — nunca concatenar na string SQL
      await ctx.client.statementExecution.executeStatement({
        warehouseId: ctx.warehouseId,
        statement: `
          INSERT INTO lakehouse_rotaperfume.gold.retorno_ligacao
            (cliente_id, vendedor, status, comentario, registrado_em, registrado_por, _referencia)
          VALUES
            (?, ?, ?, ?, current_timestamp(), ?, ?)
        `,
        parameters: [
          { name: "cliente_id", value: String(input.cliente_id), type: "INT" },
          { name: "vendedor", value: input.vendedor, type: "STRING" },
          { name: "status", value: input.status, type: "STRING" },
          { name: "comentario", value: input.comentario, type: "STRING" },
          { name: "registrado_por", value: email, type: "STRING" },
          { name: "_referencia", value: input.referencia, type: "DATE" },
        ],
      });

      res.json({ ok: true, mensagem: "Retorno registrado com sucesso" });
    } catch (err: any) {
      console.error("Erro ao gravar retorno:", err);
      res.status(500).json({
        erro: "Falha ao gravar retorno no banco",
        detalhe: err.message || String(err),
      });
    }
  });
}
