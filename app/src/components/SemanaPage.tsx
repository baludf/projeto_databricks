import React, { useState, useEffect } from "react";
import {
  useAnalyticsQuery,
  Card,
  Table,
  TableHead,
  TableRow,
  TableCell,
  TableBody,
  Select,
  Skeleton,
  Empty,
  Alert,
  Badge,
  Button,
} from "@databricks/apps";

export function SemanaPage() {
  const [vendedor, setVendedor] = useState("Todos");
  const [writeKey, setWriteKey] = useState(0);

  const { data: kpis, loading: loadingKpis, error: errorKpis } = useAnalyticsQuery("kpis_semana", { vendedor });
  const { data: vendedores, loading: loadingVendedores } = useAnalyticsQuery("vendedores");
  const { data: fila, loading: loadingFila, error: errorFila } = useAnalyticsQuery("fila", { vendedor });

  if (loadingKpis || loadingFila) return <Skeleton count={10} />;
  if (errorKpis || errorFila) return <Alert type="error">Erro ao carregar dados. Tente novamente.</Alert>;

  const kpi = kpis?.[0] || {};
  const contatos = Number(kpi.contatos) || 0;
  const vendedoresCount = Number(kpi.vendedores) || 0;
  const receitaEsperada = Number(kpi.receita_esperada) || 0;
  const liftTop200 = Number(kpi.lift_top200) || 0;
  const acertosTop200 = Number(kpi.acertos_top200) || 0;
  const taxaBase = Number(kpi.taxa_base_pct) || 0;
  const trabalhados = Number(kpi.trabalhados) || 0;
  const venderam = Number(kpi.venderam) || 0;

  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold mb-6">A semana</h1>

      {/* KPI Cards */}
      <div className="grid grid-cols-4 gap-4 mb-6">
        <Card>
          <div className="text-sm text-gray-500">Contatos da semana</div>
          <div className="text-3xl font-bold">{contatos}</div>
          <div className="text-sm text-gray-400">{vendedoresCount} vendedores</div>
        </Card>
        <Card>
          <div className="text-sm text-gray-500">Receita esperada</div>
          <div className="text-3xl font-bold">
            {receitaEsperada.toLocaleString("pt-BR", { style: "currency", currency: "BRL" })}
          </div>
        </Card>
        <Card>
          <div className="text-sm text-gray-500">Conversao prevista</div>
          <div className="text-3xl font-bold">
            {contatos > 0 ? ((acertosTop200 / contatos) * 100).toFixed(1) : 0}%
          </div>
          <div className="text-sm text-gray-400">Taxa base: {taxaBase}%</div>
        </Card>
        <Card>
          <div className="text-sm text-gray-500">Ja trabalhados</div>
          <div className="text-3xl font-bold">{trabalhados}</div>
          <div className="text-sm text-gray-400">{venderam} viraram pedido</div>
        </Card>
      </div>

      {/* Filtro vendedor */}
      <div className="mb-4">
        <Select value={vendedor} onChange={(e) => setVendedor(e.target.value)}>
          <option value="Todos">Todos os vendedores</option>
          {vendedores?.map((v: any) => (
            <option key={v.vendedor} value={v.vendedor}>
              {v.vendedor} ({v.contatos} contatos)
            </option>
          ))}
        </Select>
      </div>

      {/* Tabela da fila */}
      {(!fila || fila.length === 0) ? (
        <Empty>
          {vendedor === "Todos"
            ? "Nenhum contato na fila."
            : `${vendedor} nao possui contatos esta semana. A fila e global — contatos sao distribuidos por score, nao por cota.`}
        </Empty>
      ) : (
        <Table>
          <TableHead>
            <TableRow>
              <TableHeadCell>#</TableHeadCell>
              <TableHeadCell>Cliente</TableHeadCell>
              <TableHeadCell>Cidade</TableHeadCell>
              <TableHeadCell>Nota</TableHeadCell>
              <TableHeadCell>Faixa</TableHeadCell>
              <TableHeadCell>Motivo</TableHeadCell>
              <TableHeadCell>Sugestao</TableHeadCell>
              <TableHeadCell>Como foi a ligacao</TableHeadCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {fila?.map((row: any, idx: number) => (
              <TableRow key={`${writeKey}-${row.cliente_id}`}>
                <TableCell>{Number(row.ordem)}</TableCell>
                <TableCell>
                  <div>{row.razao_social}</div>
                  <div className="text-xs text-gray-400">{row.cidade}/{row.uf}</div>
                  <div className="text-xs text-gray-400">
                    {Number(row.ticket_medio).toLocaleString("pt-BR", { style: "currency", currency: "BRL" })}
                  </div>
                </TableCell>
                <TableCell>{row.cidade}/{row.uf}</TableCell>
                <TableCell>{(Number(row.score) * 100).toFixed(0)}%</TableCell>
                <TableCell>
                  <Badge color={row.faixa === "Muito quente" ? "red" : row.faixa === "Quente" ? "orange" : row.faixa === "Morna" ? "yellow" : "gray"}>
                    {row.faixa}
                  </Badge>
                </TableCell>
                <TableCell className="max-w-xs whitespace-normal break-words">{row.motivo}</TableCell>
                <TableCell>{row.sugestao}</TableCell>
                <TableCell>
                  {row.retorno_status ? (
                    <div>
                      <Badge color={row.retorno_status === "vendeu" ? "green" : "gray"}>
                        {row.retorno_status}
                      </Badge>
                      {row.retorno_comentario && (
                        <div className="text-xs text-gray-400 mt-1">{row.retorno_comentario}</div>
                      )}
                    </div>
                  ) : (
                    <RetornoButtons
                      clienteId={row.cliente_id}
                      vendedor={row.vendedor}
                      referencia={kpis?.[0]?.referencia_fila}
                      onGravado={() => setWriteKey((k) => k + 1)}
                    />
                  )}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
    </div>
  );
}

function RetornoButtons({ clienteId, vendedor, referencia, onGravado }: {
  clienteId: number;
  vendedor: string;
  referencia: string;
  onGravado: () => void;
}) {
  const [comentario, setComentario] = useState("");
  const [graving, setGraving] = useState(false);
  const [erro, setErro] = useState("");

  const gravar = async (status: string) => {
    setGraving(true);
    setErro("");
    try {
      const resp = await fetch("/api/retorno", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          cliente_id: clienteId,
          vendedor,
          status,
          comentario,
          referencia,
        }),
      });
      if (!resp.ok) {
        const data = await resp.json();
        setErro(data.detalhe || "Erro ao gravar retorno");
        return;
      }
      onGravado();
    } catch (e) {
      setErro("Erro de conexao");
    } finally {
      setGraving(false);
    }
  };

  return (
    <div className="flex flex-col gap-1">
      <input
        type="text"
        placeholder="Comentario..."
        value={comentario}
        onChange={(e) => setComentario(e.target.value)}
        className="text-xs border rounded px-2 py-1 w-40"
        maxLength={500}
      />
      <div className="flex gap-1">
        {["vendeu", "vai_pensar", "sem_interesse", "nao_atendeu"].map((s) => (
          <Button
            key={s}
            size="sm"
            disabled={graving}
            onClick={() => gravar(s)}
            color={s === "vendeu" ? "green" : "default"}
          >
            {s === "vendeu" ? "Vendeu" : s === "vai_pensar" ? "Vai pensar" : s === "sem_interesse" ? "Sem interesse" : "Nao atendeu"}
          </Button>
        ))}
      </div>
      {erro && <Alert type="error" className="mt-1">{erro}</Alert>}
    </div>
  );
}
