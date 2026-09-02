import React, { useEffect, useState } from "react";
import { Card, Skeleton, Alert } from "@databricks/apps";

export function PerguntarPage() {
  const [email, setEmail] = useState("");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/quem-sou")
      .then((r) => r.json())
      .then((data) => {
        setEmail(data.email || "usuario@exemplo.com");
        setLoading(false);
      })
      .catch(() => {
        setEmail("usuario@exemplo.com");
        setLoading(false);
      });
  }, []);

  if (loading) return <Skeleton count={5} />;

  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold mb-4">Perguntar</h1>
      <Card className="mb-4">
        <div className="text-sm text-gray-500">
          Logado como: <span className="font-medium">{email}</span>
        </div>
      </Card>
      <Alert type="info" className="mb-4">
        As respostas sao geradas por IA e incluem o SQL que as produziu. Sempre verifique os numeros antes de usa-los.
      </Alert>
      <div className="border rounded-lg p-4 bg-gray-50">
        <p className="text-gray-600">
          Genie Chat do space "Rota do Perfume · Direção" sera embutido aqui.
          <br />
          Pergunte sobre a fila, metricas, clientes ou qualquer coisa do negocio.
        </p>
      </div>
    </div>
  );
}
