import React from "react";
import { createApp } from "@databricks/apps";

import { SemanaPage } from "./components/SemanaPage";
import { PerguntarPage } from "./components/PerguntarPage";
import { AcompanhamentoPage } from "./components/AcompanhamentoPage";

const app = createApp({
  cache: { enabled: false },
  routes: [
    { path: "/", element: <SemanaPage /> },
    { path: "/perguntar", element: <PerguntarPage /> },
    { path: "/acompanhamento", element: <AcompanhamentoPage /> },
  ],
});

export default app;
