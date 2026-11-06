# CosmoTech — Orders Worker

Consumidor de mensagens **Amazon SQS** responsável pelo processamento assíncrono de pedidos no laboratório EKS. Foi separado de `cosmotech-orders-api` com preservação do histórico Git.

Os módulos Orders compartilhados e testes de contrato permanecem como snapshot fixado até a criação de um pacote compartilhado versionado. Este repositório mantém `Dockerfile.worker` e o runtime do Worker; o código da API mantido aqui serve apenas aos testes de contrato.

## Como funciona a entrega DEV

Push em `main` → lint/testes → build Docker → Trivy → publicação no ECR via OIDC temporário → proposta GitOps pelo GitHub App → promoção DEV validada → sincronização Argo CD → EKS.

O Actions não modifica Deployments EKS. Pull requests apenas validam, constroem e analisam imagens, sem publicá-las. A entrega DEV está habilitada sem aprovação humana.

## Configurações necessárias

- `ECR_PUBLISH_ROLE_ARN`: role OIDC exclusiva do Worker.
- `GITOPS_APP_ID` e `GITOPS_APP_PRIVATE_KEY`: GitHub App instalado somente em `cosmotech-gitops`.
- `DEV_AUTO_PROMOTE=true`: permite mesclagem automática **após** validação de alteração exclusiva de imagem.

Configuração obrigatória ausente faz a entrega falhar; não é ignorada silenciosamente. Mudanças de código ou workflow em `main` iniciam CI.

## Saúde e processamento

Endpoint: https://orders-worker-dev.cosmotechsolutions.com.br/health/ready

O host atende apenas GETs de saúde. Requisições de negócio devem ser enviadas à Orders API, que publica mensagens na SQS. A readiness depende de polling SQS bem-sucedido e fica indisponível em erros da fila, desligamento ou 90 segundos sem progresso. A liveness também detecta loop travado.

O HPA usa CPU do contêiner como métrica inicial; para escalar por backlog SQS é necessário adaptador de métricas externas. O histórico anterior documentava 1–4 réplicas, mas a seção **Capacidade DEV** abaixo descreve o padrão mais recente de duas réplicas fixas; confirme sempre o manifesto vigente.

Mantêm-se idempotência, retries limitados do SDK, visibilidade SQS de 180 segundos e DLQ. Não há migração de tabelas/filas nem nova retenção de dados.

## Validação e rollback

Após publicar, confira Argo CD `Synced/Healthy`, HPA `AbleToScale/ScalingActive`, TLS do endpoint, idade das mensagens, DLQ e erros de processamento. Alertas da fila continuam na stack de observabilidade.

Para reverter, restaure o digest da imagem validada anteriormente pelo GitOps. **Não limpe a fila nem altere o inventário DynamoDB.** Réplicas adicionais podem elevar capacidade EKS e custos de chamadas AWS. Artefatos de imagem do CI são retidos por dois dias; evidências da promoção seguem a retenção padrão de artefatos GitHub.

## Configuração e entrega DEV

`deploy/dev` neste repositório é a origem oficial de Deployment, ConfigMap, Service, HPA e host. Um push em `main` executa testes, valida essas configurações, constrói e verifica a imagem, publica no ECR e promove automaticamente o conjunto completo para o espelho gerado de entrega GitOps. Argo CD reconcilia o cluster e Kestra mostra as etapas reais por cluster. Não há aprovação humana nesta fase. Não edite o espelho gerado para configurar esta aplicação.

## Inicialização e disponibilidade no host

A startup probe controla a inicialização antes de habilitar readiness/liveness. No worker, ela só passa após a primeira consulta SQS bem-sucedida; a primeira verificação ocorre após a espera normal de long polling (20 segundos). Falhas reais de acesso à fila continuam retornando 503. O rollout mantém a réplica antiga até a nova ficar pronta, usando maxUnavailable=0, maxSurge=1 e a readiness gate do ALB habilitada no namespace pela configuração de infraestrutura `ops/kubernetes/orders-ingress-readiness.json`.

## Logs e conexões sintéticas

Um ciclo por pod a cada 300 segundos registra heartbeat JSON, chama a rota de diagnóstico da API e faz GET HTTPS em `example.com`. GET `/diagnostics/ping` registra chamadas internas/externas com spans de servidor; os clientes propagam W3C traceparent e registram status, duração e trace ID. Nenhum teste gera pedido de negócio. Os alvos são fixos, os timeouts são limitados e corpo de resposta/credenciais ficam fora dos logs. O estado de prontidão do polling SQS continua sendo avaliado separadamente pelos endpoints de saúde.

O APM exporta métricas OTLP reais de volume e duração por operação, com atributos limitados a operação, tipo e estado. Exportação assíncrona a cada 60 segundos; chamadas sintéticas continuam a cada 5 minutos. Logs estruturados seguem pelo Alloy para Loki e Zabbix.

## Capacidade DEV

A release 1.0.6 define duas réplicas estáveis pelo HPA (`minReplicas=2`, `maxReplicas=2`). O HPA mantém a propriedade da escala; não há `spec.replicas` concorrente no manifesto. A mudança passa por CI, GMUD e aprovação conforme a política de criticidade e alterações sensíveis. No Worker, o Canary continua limitado a1→2 consumidores temporários, com promoção, drenagem e restauração da capacidade aprovada.
