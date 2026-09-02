import React from "react";
import { useAnalyticsQuery, Card, Table, TableHead, TableRow, TableCell, TableBody, Skeleton, Empty, Alert } from "@databricks/apps";

export function AcompanhamentoPage() {
  const { data, loading, error } = useAnalyticsQuery("acompanhamento");

  if (loading) return <Skeleton count={10} />;
  if (error) return <Alert type="error">Erro ao carregar acompanhamento.</Alert>;

  const totalNaFila = data?.reduce((acc: number, r: any) => acc + Number(r.na_fila), 0) || 0;
  const totalTrabalhados = data?.reduce((acc: number, r: any) => acc + Number(r.trabalhados), 0) || 0;
  const totalVendeu = data?.reduce((acc: number, r: any) => acc + Number(r.vendeu), 0) || 0;

  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold mb-6">Acompanhamento</h1>

      {totalTrabalhados === 0 ? (
        <Empty>
          Nenhum retorno registrado ainda. Assim que o time marcar o retorno,
          os numeros aparecero aqui. Isso vira dado de treino da semana que vem.
        </Empty>
      ) : (
        <>
          <Card className="mb-6">
            <div className="text-lg">
              <span className="font-bold">{totalTrabalhados}</span> dos <span className="font-bold">{totalNaFila}</span> contatos foram trabalhados.
              {" "}<span className="font-bold">{totalVendeu}</span> viraram pedido.
            </div>
          </Card>

          <Table>
            <TableHead>
              <TableRow>
                <TableHeadCell>Vendedor</TableHeadCell>
                <TableHeadCell>Na fila</TableHeadCell>
                <TableHeadCell>Trabalhados</TableHeadCell>
                <TableHeadCell>Vendeu</TableHeadCell>
                <TableHeadCell>Vai pensar</TableHeadCell>
                <TableHeadCell>Sem interesse</TableHeadCell>
                <TableHeadCell>Nao atendeu</TableHeadCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {data?.map((row: any) => (
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
