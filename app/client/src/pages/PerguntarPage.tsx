import { useEffect, useState } from "react";
import { Card, CardContent, Skeleton, Alert, AlertDescription } from "@databricks/appkit-ui/react";

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

  if (loading) return <Skeleton className="h-32 w-full" />;

  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold mb-4">Perguntar</h1>
      <Card className="mb-4">
        <CardContent>
          <div className="text-sm text-muted-foreground">
            Logado como: <span className="font-medium">{email}</span>
          </div>
        </CardContent>
      </Card>
      <Alert variant="default" className="mb-4">
        <AlertDescription>
          As respostas são geradas por IA e incluem o SQL que as produziu. Sempre verifique os números antes de usá-los.
        </AlertDescription>
      </Alert>
      <div className="border rounded-lg p-4 bg-muted">
        <p className="text-muted-foreground">
          Genie Chat do space "Rota do Perfume · Direção" será embutido aqui.
          <br />
          Pergunte sobre a fila, métricas, clientes ou qualquer coisa do negócio.
        </p>
      </div>
    </div>
  );
}
