import { useState, useMemo } from "react";
import { useAnalyticsQuery } from "@databricks/appkit-ui/react";
import { sql } from "@databricks/appkit-ui/js";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Skeleton,
  Empty,
  Alert,
  Badge,
  Button,
} from "@databricks/appkit-ui/react";

interface KpiRow {
  contatos: number;
  vendedores: number;
  receita_esperada: number;
  lift_top200: number;
  acertos_top200: number;
  taxa_base_pct: number;
  referencia_fila: string;
  trabalhados: number;
  venderam: number;
}

interface VendedorRow {
  vendedor: string;
  contatos: number;
}

interface FilaRow {
  vendedor: string;
  ordem: number;
  cliente_id: number;
  razao_social: string;
  cidade: string;
  uf: string;
  score: number;
  faixa: string;
  ticket_medio: number;
  motivo: string;
  sugestao: string;
  retorno_status: string | null;
  retorno_comentario: string | null;
}

export function SemanaPage() {
  const [vendedor, setVendedor] = useState("Todos");
  const [writeKey, setWriteKey] = useState(0);

  const vendedorParams = useMemo(() => ({ vendedor: sql.string(vendedor), w: sql.number(writeKey) }), [vendedor, writeKey]);
  const todosParams = useMemo(() => ({}), []);

  const { data: kpis, loading: loadingKpis, error: errorKpis } = useAnalyticsQuery("kpis_semana", vendedorParams);
  const { data: vendedores, loading: _loadingVendedores } = useAnalyticsQuery("vendedores", todosParams);
  const { data: fila, loading: loadingFila, error: errorFila } = useAnalyticsQuery("fila", vendedorParams);

  if (loadingKpis || loadingFila) return <Skeleton className="h-32 w-full" />;
  if (errorKpis || errorFila) return <Alert variant="destructive">Erro ao carregar dados. Tente novamente.</Alert>;


  const kpiRows = (kpis ?? []) as KpiRow[];
  const kpi = kpiRows[0] ?? ({} as KpiRow);
  const vendedorRows = (vendedores ?? []) as VendedorRow[];
  const filaRows = (fila ?? []) as FilaRow[];

  const contatos = Number(kpi.contatos) || 0;
  const vendedoresCount = Number(kpi.vendedores) || 0;
  const receitaEsperada = Number(kpi.receita_esperada) || 0;
  const acertosTop200 = Number(kpi.acertos_top200) || 0;
  const taxaBase = Number(kpi.taxa_base_pct) || 0;
  const trabalhados = Number(kpi.trabalhados) || 0;
  const venderam = Number(kpi.venderam) || 0;

  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold mb-6">A semana</h1>

      <div className="grid grid-cols-4 gap-4 mb-6">
        <Card>
          <CardHeader>
            <CardTitle>Contatos da semana</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold">{contatos}</div>
            <div className="text-sm text-muted-foreground">{vendedoresCount} vendedores</div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Receita esperada</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold">
              {receitaEsperada.toLocaleString("pt-BR", { style: "currency", currency: "BRL" })}
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Conversão prevista</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold">
              {contatos > 0 ? ((acertosTop200 / contatos) * 100).toFixed(1) : 0}%
            </div>
            <div className="text-sm text-muted-foreground">Taxa base: {taxaBase}%</div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Já trabalhados</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold">{trabalhados}</div>
            <div className="text-sm text-muted-foreground">{venderam} viraram pedido</div>
          </CardContent>
        </Card>
      </div>

      <div className="mb-4">
        <Select value={vendedor} onValueChange={(value: string) => setVendedor(value)}>
          <SelectTrigger className="w-[280px]">
            <SelectValue placeholder="Todos os vendedores" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="Todos">Todos os vendedores</SelectItem>
            {vendedorRows.map((v) => (
              <SelectItem key={v.vendedor} value={v.vendedor}>
                {v.vendedor} ({v.contatos} contatos)
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {filaRows.length === 0 ? (
        <Empty>
          {vendedor === "Todos"
            ? "Nenhum contato na fila."
            : `${vendedor} não possui contatos esta semana. A fila é global — contatos são distribuídos por score, não por cota.`}
        </Empty>
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>#</TableHead>
              <TableHead>Cliente</TableHead>
              <TableHead>Cidade</TableHead>
              <TableHead>Nota</TableHead>
              <TableHead>Faixa</TableHead>
              <TableHead>Motivo</TableHead>
              <TableHead>Sugestão</TableHead>
              <TableHead>Como foi a ligação</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {filaRows.map((row) => (
              <TableRow key={`${writeKey}-${row.cliente_id}`}>
                <TableCell>{row.ordem}</TableCell>
                <TableCell>
                  <div>{row.razao_social}</div>
                  <div className="text-xs text-muted-foreground">{row.cidade}/{row.uf}</div>
                  <div className="text-xs text-muted-foreground">
                    {row.ticket_medio.toLocaleString("pt-BR", { style: "currency", currency: "BRL" })}
                  </div>
                </TableCell>
                <TableCell>{row.cidade}/{row.uf}</TableCell>
                <TableCell>{(row.score * 100).toFixed(0)}%</TableCell>
                <TableCell>
                  <Badge variant={row.faixa === "Muito quente" ? "destructive" : row.faixa === "Quente" ? "secondary" : "default"}>
                    {row.faixa}
                  </Badge>
                </TableCell>
                <TableCell className="max-w-xs whitespace-normal break-words">{row.motivo}</TableCell>
                <TableCell>{row.sugestao}</TableCell>
                <TableCell>
                  {row.retorno_status ? (
                    <div>
                      <Badge variant={row.retorno_status === "vendeu" ? "default" : "secondary"}>
                        {row.retorno_status === "vendeu" ? "Vendeu" : row.retorno_status === "vai_pensar" ? "Vai pensar" : row.retorno_status === "sem_interesse" ? "Sem interesse" : "Não atendeu"}
                      </Badge>
                      {row.retorno_comentario && (
                        <div className="text-xs text-muted-foreground mt-1">{row.retorno_comentario}</div>
                      )}
                    </div>
                  ) : (
                    <RetornoButtons
                      clienteId={row.cliente_id}
                      vendedor={row.vendedor}
                      referencia={kpi.referencia_fila}
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
  clienteId: number | string;
  vendedor: string;
  referencia: string | null;
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
          cliente_id: Number(clienteId),
          vendedor,
          status,
          comentario,
          referencia: referencia ?? "",
        }),
      });
      if (!resp.ok) {
        const data = await resp.json();
        setErro(data.detalhe || "Erro ao gravar retorno");
        return;
      }
      onGravado();
    } catch {
        setErro("Erro de conexão");
    } finally {
      setGraving(false);
    }
  };

  return (
    <div className="flex flex-col gap-1">
      <input
        type="text"
        placeholder="Comentário..."
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
            variant={s === "vendeu" ? "default" : "secondary"}
          >
            {s === "vendeu" ? "Vendeu" : s === "vai_pensar" ? "Vai pensar" : s === "sem_interesse" ? "Sem interesse" : "Não atendeu"}
          </Button>
        ))}
      </div>
      {erro && <Alert variant="destructive" className="mt-1">{erro}</Alert>}
    </div>
  );
}
