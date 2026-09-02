import { useMemo } from "react";
import { useAnalyticsQuery } from "@databricks/appkit-ui/react";
import {
  Card,
  CardContent,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  Skeleton,
  Empty,
  Alert,
} from "@databricks/appkit-ui/react";

interface AcompanhamentoRow {
  vendedor: string;
  na_fila: number;
  trabalhados: number;
  vendeu: number;
  vai_pensar: number;
  sem_interesse: number;
  nao_atendeu: number;
}

export function AcompanhamentoPage() {
  const params = useMemo(() => ({}), []);
  const { data, loading, error } = useAnalyticsQuery("acompanhamento", params);

  if (loading) return <Skeleton className="h-32 w-full" />;
  if (error) return <Alert variant="destructive">Erro ao carregar acompanhamento.</Alert>;

  const rows = (data ?? []) as AcompanhamentoRow[];
  const totalNaFila = rows.reduce((acc, r) => acc + Number(r.na_fila), 0);
  const totalTrabalhados = rows.reduce((acc, r) => acc + Number(r.trabalhados), 0);
  const totalVendeu = rows.reduce((acc, r) => acc + Number(r.vendeu), 0);

  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold mb-6">Acompanhamento</h1>

      {totalTrabalhados === 0 ? (
        <Empty>
          Nenhum retorno registrado ainda. Assim que o time marcar o retorno,
          os números aparecerão aqui. Isso vira dado de treino da semana que vem.
        </Empty>
      ) : (
        <>
          <Card className="mb-6">
            <CardContent>
              <div className="text-lg">
                <span className="font-bold">{totalTrabalhados}</span> dos <span className="font-bold">{totalNaFila}</span> contatos foram trabalhados.
                {" "}<span className="font-bold">{totalVendeu}</span> viraram pedido.
              </div>
            </CardContent>
          </Card>

          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Vendedor</TableHead>
                <TableHead>Na fila</TableHead>
                <TableHead>Trabalhados</TableHead>
                <TableHead>Vendeu</TableHead>
                <TableHead>Vai pensar</TableHead>
                <TableHead>Sem interesse</TableHead>
                <TableHead>Não atendeu</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.map((row) => (
                <TableRow key={row.vendedor}>
                  <TableCell>{row.vendedor}</TableCell>
                  <TableCell>{Number(row.na_fila)}</TableCell>
                  <TableCell>{Number(row.trabalhados)}</TableCell>
                  <TableCell>{Number(row.vendeu)}</TableCell>
                  <TableCell>{Number(row.vai_pensar)}</TableCell>
                  <TableCell>{Number(row.sem_interesse)}</TableCell>
                  <TableCell>{Number(row.nao_atendeu)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </>
      )}
    </div>
  );
}
